//! Proving a request genuinely came from who it claims: a provider's
//! push, or the caller of a gated route.
//!
//! Each scheme in [`VerifyKind`] is a PROTOCOL: the arithmetic (an
//! HMAC, a constant-time compare, a public-key or OIDC signature
//! check) is implemented here once, and everything provider-shaped
//! about it (where the signature sits and how it is laid out, what is
//! signed, how the secret is written) is data the recipe declares. The
//! SECRET material never lives in the recipe: it is handed in per call
//! by whoever holds it (the app the operator registered, the connection
//! a route is gated by, or the token weft minted when it subscribed).
//!
//! Everything here is PURE. The signed-identity scheme needs the
//! issuer's published keys, which is I/O: that fetch belongs to the
//! side holding the network, and this module owns the two halves it
//! can decide alone ([`needs_issuer_keys`] and
//! [`check_identity_claims`]).

use std::cell::OnceCell;
use std::collections::BTreeMap;

use base64::Engine as _;
// Both signature crates re-export the same `signature` crate's
// Verifier trait, so this one import serves ed25519 and ECDSA.
use ed25519_dalek::Verifier as _;
use hmac::{Hmac, KeyInit, Mac};
use p256::pkcs8::DecodePublicKey as _;
use serde_json::Value;
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use super::events::{
    header_value, single_value, Ambiguous, DigestEncoding, HmacAlgorithm, KeyEncoding, RequestValue, SecretKey,
    SignatureScheme, SignatureSource, SignedConcat, SignedPart, SignedSegment, TimestampFormat, TimestampSource,
    VerifyKind,
};
use super::{hex_of, hex_to_bytes};

/// What one scheme needs beyond the request itself. Assembled by the
/// side that holds the material (the broker, from the receiving app's
/// configuration, from the subscription row, or from the connection a
/// route is gated by), never by the sender of the request.
#[derive(Debug, Clone, Default)]
pub struct VerifySecrets {
    /// The shared secret an HMAC scheme keys on, as stored (the
    /// scheme's `secret` says how it becomes the key).
    pub signing_secret: Option<String>,
    /// The public key a signature scheme verifies against (not a
    /// secret, but configured in the same place: on the app that
    /// receives). Hex for ed25519, base64 DER for ecdsa_p256.
    pub public_key: Option<String>,
    /// The token weft minted when it subscribed, which a
    /// no-signature provider echoes back on every push.
    pub minted_token: Option<String>,
    /// Where that echoed token arrives, as a path in the events
    /// recipe's vocabulary (a `header:` path for every provider
    /// surveyed). Read off the push by the caller, which is the side
    /// holding the recipe.
    pub presented_token: Option<String>,
    /// The audience a signed identity token must name.
    pub audience: Option<String>,
    /// The issuer identity a signed identity token must carry
    /// (the account configured to push).
    pub expected_email: Option<String>,
    /// The keys a caller may present, for the shared-key scheme.
    pub api_keys: Option<Vec<String>>,
}

/// The material a scheme reads off a CONNECTION's stored values, by
/// the fixed names the scheme vocabulary reserves
/// (`signingSecret`, `publicKey`, `keys`, `audience`). Pure: the
/// broker hands the connection's values in, the scheme's secrets come
/// out. A key list is keys separated by commas or line breaks (the
/// paste field is one line; a key never contains either), blanks
/// dropped.
pub fn secrets_from_values(values: &BTreeMap<String, String>) -> VerifySecrets {
    use super::events::{API_KEYS, AUDIENCE, PUBLIC_KEY, SIGNING_SECRET};
    VerifySecrets {
        signing_secret: values.get(SIGNING_SECRET).cloned(),
        public_key: values.get(PUBLIC_KEY).cloned(),
        audience: values.get(AUDIENCE).cloned(),
        api_keys: values.get(API_KEYS).map(|raw| {
            raw.split([',', '\n', '\r'])
                .map(str::trim)
                .filter(|l| !l.is_empty())
                .map(str::to_string)
                .collect()
        }),
        ..Default::default()
    }
}

/// Why a request was refused. Never echoed to the sender in detail (a
/// verifier that says WHICH part failed is an oracle); logged where
/// the operator reads and answered as a flat refusal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("the request carries no {0}")]
    Missing(String),
    #[error(transparent)]
    Ambiguous(#[from] Ambiguous),
    #[error("the request's signature does not match")]
    BadSignature,
    #[error("the request's {0} is not a timestamp in the format the scheme declares")]
    BadTimestamp(String),
    #[error("the request's timestamp is {0}s away from now, outside the replay window")]
    StaleTimestamp(u64),
    #[error("no {0} is configured for this service, so its requests cannot be verified")]
    NotConfigured(&'static str),
    #[error("the request's identity token is not usable: {0}")]
    BadIdentityToken(String),
    #[error("the issuer's signing keys cannot be fetched: {0}")]
    UnusableKeyAddress(String),
}

/// One raw request, as the schemes see it: the EXACT bytes received
/// (every signing scheme signs the bytes, so a parsed-and-reserialized
/// body would never match), every header as sent (a repeated header
/// keeps each of its values), and, for the schemes that sign the
/// request address (Twilio's), the full address it was sent to and
/// how it was called.
#[derive(Debug, Clone, Copy)]
pub struct PushParts<'a> {
    pub body: &'a [u8],
    pub headers: &'a [(String, String)],
    /// The full public address the request was sent to (path and query
    /// included); `None` when the receiver cannot know it (an
    /// address-signing scheme then refuses as unconfigured, never
    /// verifies blind).
    pub url: Option<&'a str>,
    pub method: &'a str,
}

/// A request being verified: its parts, the JSON body parsed at most
/// once (only when a body field is read), and the timestamp as the
/// request wrote it once it has been read.
struct Request<'p, 'a> {
    push: &'p PushParts<'a>,
    json: OnceCell<Option<Value>>,
}

impl<'p, 'a> Request<'p, 'a> {
    fn new(push: &'p PushParts<'a>) -> Self {
        Self { push, json: OnceCell::new() }
    }

    /// Every value of the header `name`, in the order sent.
    fn header_values(&self, name: &str) -> impl Iterator<Item = &'a str> + '_ {
        let name = name.to_string();
        self.push.headers.iter().filter(move |(k, _)| k.eq_ignore_ascii_case(&name)).map(|(_, v)| v.as_str())
    }

    /// The one value of the header `name` ([`header_value`]'s rule: sent
    /// twice with the same value it is still one, sent with two it is
    /// ambiguous); a header not sent is missing.
    fn header(&self, name: &str) -> Result<&'a str, VerifyError> {
        header_value(self.push.headers, name)?.ok_or_else(|| VerifyError::Missing(format!("{name} header")))
    }

    /// One field of the JSON body, as signed text: a string as is, a
    /// number or boolean as written. Anything else (absent, null, an
    /// object, a body that is not JSON) is missing.
    fn body_field(&self, path: &str) -> Result<String, VerifyError> {
        let json = self.json.get_or_init(|| serde_json::from_slice(self.push.body).ok());
        let missing = || VerifyError::Missing(format!("body field {path}"));
        match json.as_ref().and_then(|body| super::spec::lookup_path(body, path)) {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(v @ (Value::Number(_) | Value::Bool(_))) => Ok(v.to_string()),
            _ => Err(missing()),
        }
    }

    /// A header's or a body field's value.
    fn value(&self, at: &RequestValue) -> Result<String, VerifyError> {
        match at {
            RequestValue::Header(name) => self.header(name).map(str::to_string),
            RequestValue::BodyField(path) => self.body_field(path),
            // Refused at validation for every place a value is read
            // this way; a layout key is read through the layout.
            RequestValue::SignatureKey(key) => Err(VerifyError::Missing(format!("signature key {key}"))),
        }
    }

    /// The signature value(s) as they arrived, before the layout cuts
    /// them: every copy of a header, or the one body field.
    fn signature_values(&self, signature: &SignatureSource) -> Result<Vec<String>, VerifyError> {
        let values: Vec<String> = match &signature.at {
            RequestValue::Header(name) => self.header_values(name).map(str::to_string).collect(),
            other => vec![self.value(other)?],
        };
        if values.is_empty() {
            return Err(VerifyError::Missing(format!("{} (the signature)", signature.at)));
        }
        Ok(values)
    }
}

/// The entries of one signature value under its layout, as
/// `(key, value)`: the key is `None` when the entries are bare. An
/// entry without the pair separator has no key and is skipped.
fn entries<'v>(raw: &'v str, layout: &SignatureSource) -> Vec<(Option<&'v str>, &'v str)> {
    let listed: Vec<&str> = match &layout.list {
        Some(sep) => raw.split(sep.as_str()).collect(),
        None => vec![raw],
    };
    listed
        .into_iter()
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .filter_map(|entry| match &layout.pair {
            Some(sep) => entry.split_once(sep.as_str()).map(|(k, v)| (Some(k.trim()), v.trim())),
            None => Some((None, entry)),
        })
        .collect()
}

/// Every signature the request presents: the entries under the
/// layout's signature key (all of them, when bare), across every copy
/// of the value.
fn signature_candidates(values: &[String], layout: &SignatureSource) -> Vec<String> {
    values
        .iter()
        .flat_map(|raw| entries(raw, layout))
        .filter(|(key, _)| *key == layout.key.as_deref())
        .map(|(_, value)| value.to_string())
        .collect()
}

/// Read the timestamp a scheme signs, check it against the replay
/// window, and answer it as the request wrote it (what `{timestamp}`
/// splices). `None` for a scheme that signs no timestamp.
fn checked_timestamp(
    source: Option<&TimestampSource>,
    request: &Request<'_, '_>,
    signature: &SignatureSource,
    signature_values: &[String],
    now_unix: i64,
) -> Result<Option<String>, VerifyError> {
    let Some(source) = source else { return Ok(None) };
    let raw = match &source.at {
        RequestValue::SignatureKey(key) => {
            let found = signature_values
                .iter()
                .flat_map(|raw| entries(raw, signature))
                .filter(|(k, _)| *k == Some(key.as_str()))
                .map(|(_, v)| v);
            single_value(found, || format!("signature key {key}"))?
                .ok_or_else(|| VerifyError::Missing(format!("signature key {key}")))?
                .to_string()
        }
        other => request.value(other)?,
    };
    let sent = match source.format {
        TimestampFormat::Seconds => raw.trim().parse::<i64>().ok(),
        TimestampFormat::Milliseconds => raw.trim().parse::<i64>().ok().map(|ms| ms.div_euclid(1000)),
        TimestampFormat::Rfc3339 => chrono::DateTime::parse_from_rfc3339(raw.trim()).ok().map(|t| t.timestamp()),
    }
    .ok_or_else(|| VerifyError::BadTimestamp(source.at.to_string()))?;
    // `sent` is whatever the request wrote, so the distance is taken
    // without arithmetic that could overflow on an extreme value.
    let drift = now_unix.abs_diff(sent);
    if drift > u64::from(source.tolerance_secs) {
        return Err(VerifyError::StaleTimestamp(drift));
    }
    Ok(Some(raw))
}

/// Decode one presented signature or digest; `None` when it is not
/// written in the declared encoding (it then matches nothing).
fn decoded(presented: &str, encoding: DigestEncoding) -> Option<Vec<u8>> {
    match encoding {
        DigestEncoding::Hex => hex_to_bytes(presented.trim()),
        DigestEncoding::Base64 => LENIENT_BASE64.decode(presented.trim()).ok(),
    }
}

/// Standard base64, read with or without its `=` padding.
const LENIENT_BASE64: base64::engine::GeneralPurpose = base64::engine::GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    base64::engine::GeneralPurposeConfig::new().with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
);

/// The HMAC key a stored secret stands for, per the scheme's `secret`.
pub fn hmac_key(secret: &SecretKey, stored: &str) -> Result<Vec<u8>, VerifyError> {
    let rest = stored.strip_prefix(secret.strip_prefix.as_str()).ok_or(VerifyError::NotConfigured(
        "usable signing secret (it lacks the prefix the scheme says the provider's secrets carry; paste it whole)",
    ))?;
    match secret.decode {
        KeyEncoding::Raw => Ok(rest.as_bytes().to_vec()),
        KeyEncoding::Base64 => LENIENT_BASE64
            .decode(rest.trim())
            .map_err(|_| VerifyError::NotConfigured("usable signing secret (it is not valid base64)")),
        KeyEncoding::Hex => hex_to_bytes(rest.trim())
            .ok_or(VerifyError::NotConfigured("usable signing secret (it is not valid hex)")),
    }
}

fn hmac_bytes(key: &[u8], algorithm: HmacAlgorithm, message: &[u8]) -> Vec<u8> {
    match algorithm {
        HmacAlgorithm::Sha256 => {
            let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("hmac accepts a key of any length");
            mac.update(message);
            mac.finalize().into_bytes().to_vec()
        }
        HmacAlgorithm::Sha1 => {
            let mut mac = <Hmac<Sha1>>::new_from_slice(key).expect("hmac accepts a key of any length");
            mac.update(message);
            mac.finalize().into_bytes().to_vec()
        }
        HmacAlgorithm::Sha512 => {
            let mut mac = <Hmac<Sha512>>::new_from_slice(key).expect("hmac accepts a key of any length");
            mac.update(message);
            mac.finalize().into_bytes().to_vec()
        }
    }
}

/// Run the scheme a service declares against one raw request. Answers
/// the IDENTITY the scheme established: which key matched (`{"key":
/// 3}`) for the shared-key scheme, and an empty object for the
/// schemes that prove origin without naming anyone.
///
/// Pure. The signed-identity scheme needs the issuer's published
/// keys, so it is not decided here; see [`needs_issuer_keys`].
pub fn verify_push(
    kind: &VerifyKind,
    push: &PushParts<'_>,
    secrets: &VerifySecrets,
    now_unix: i64,
) -> Result<serde_json::Value, VerifyError> {
    let anonymous = serde_json::json!({});
    let request = Request::new(push);
    match kind {
        VerifyKind::Hmac { .. } | VerifyKind::Signature { .. } => {
            SignedPush::read(kind, push, now_unix)?.check(secrets).map(|()| anonymous)
        }
        VerifyKind::TokenEcho => {
            // The provider signs nothing; it echoes back the secret
            // weft minted when it asked for the subscription. That
            // token is per-subscription and unguessable, so matching
            // it is the whole proof.
            let minted = secrets
                .minted_token
                .as_deref()
                .ok_or(VerifyError::NotConfigured("subscription token"))?;
            let presented = secrets
                .presented_token
                .as_deref()
                .ok_or_else(|| VerifyError::Missing("subscription token".into()))?;
            if constant_time_eq(presented.as_bytes(), minted.as_bytes()) {
                Ok(anonymous)
            } else {
                Err(VerifyError::BadSignature)
            }
        }
        VerifyKind::ApiKeys { header: key_header, bearer } => {
            let keys = secrets
                .api_keys
                .as_deref()
                .filter(|k| !k.is_empty())
                .ok_or(VerifyError::NotConfigured("api key list"))?;
            // The key rides its own header, or the bearer slot when the
            // scheme allows it; the header wins when both are present.
            let presented = match request.header(key_header) {
                Ok(value) => value.trim(),
                Err(VerifyError::Missing(_)) if *bearer => bearer_token(push.headers)
                    .ok_or_else(|| VerifyError::Missing(format!("{key_header} header")))?,
                Err(e) => return Err(e),
            };
            // Every key is compared (no early exit on the first match)
            // so the time taken says nothing about which key matched.
            let mut matched: Option<usize> = None;
            for (i, key) in keys.iter().enumerate() {
                if constant_time_eq(presented.as_bytes(), key.as_bytes()) && matched.is_none() {
                    matched = Some(i);
                }
            }
            match matched {
                Some(i) => Ok(serde_json::json!({ "key": i })),
                None => Err(VerifyError::BadSignature),
            }
        }
        VerifyKind::Oidc { .. } => Err(VerifyError::NotConfigured(
            "signed identity tokens are verified against the issuer's published keys; \
             use the async identity-token path",
        )),
    }
}

/// A push under a signing scheme (`hmac` or `signature`), read: the exact
/// bytes it signs and every signature it presents, with its timestamp
/// already held to the replay window. Reading it needs no secret, so a
/// failure here is the REQUEST's (a missing, repeated or stale part) and
/// holds whoever checks it; [`SignedPush::check`] is the one step that
/// depends on the material, so a receiver holding several candidates
/// (one receiving app each) reads the push once and checks each.
pub struct SignedPush<'k> {
    check: SigningCheck<'k>,
    message: Vec<u8>,
    candidates: Vec<String>,
}

/// What a signing scheme does with its material, the one part of it
/// [`SignedPush::check`] still needs once the push is read.
enum SigningCheck<'k> {
    Hmac { secret: &'k SecretKey, algorithm: HmacAlgorithm, encoding: DigestEncoding },
    Signature { scheme: SignatureScheme, encoding: DigestEncoding },
}

impl<'k> SignedPush<'k> {
    /// Read `push` under `kind`, which must be a signing scheme.
    pub fn read(kind: &'k VerifyKind, push: &PushParts<'_>, now_unix: i64) -> Result<Self, VerifyError> {
        let (check, signature, timestamp, concat) = match kind {
            VerifyKind::Hmac { signature, timestamp, concat, secret, algorithm, encoding } => {
                (SigningCheck::Hmac { secret, algorithm: *algorithm, encoding: *encoding }, signature, timestamp, concat)
            }
            VerifyKind::Signature { scheme, signature, timestamp, concat, encoding } => {
                (SigningCheck::Signature { scheme: *scheme, encoding: *encoding }, signature, timestamp, concat)
            }
            VerifyKind::TokenEcho | VerifyKind::Oidc { .. } | VerifyKind::ApiKeys { .. } => {
                return Err(VerifyError::NotConfigured("signing scheme (hmac or signature) to read the push under"));
            }
        };
        let request = Request::new(push);
        let values = request.signature_values(signature)?;
        let candidates = signature_candidates(&values, signature);
        if candidates.is_empty() {
            return Err(VerifyError::Missing(format!("signature under {}", signature.at)));
        }
        let sent = checked_timestamp(timestamp.as_ref(), &request, signature, &values, now_unix)?;
        let message = resolve_concat(concat, sent.as_deref(), &request)?;
        Ok(Self { check, message, candidates })
    }

    /// Whether one presented signature verifies under `secrets`: the HMAC
    /// keyed by its signing secret, or the signature against its public
    /// key. A failure here is the material's (none configured, or not in
    /// the form the scheme needs) or a signature that does not match.
    pub fn check(&self, secrets: &VerifySecrets) -> Result<(), VerifyError> {
        let matched = match &self.check {
            SigningCheck::Hmac { secret, algorithm, encoding } => {
                let stored = secrets.signing_secret.as_deref().ok_or(VerifyError::NotConfigured("signing secret"))?;
                let expected = hmac_bytes(&hmac_key(secret, stored)?, *algorithm, &self.message);
                // Every candidate is compared (no early exit), so the time
                // taken says nothing about which one matched.
                let mut matched = false;
                for candidate in &self.candidates {
                    let presented = decoded(candidate, *encoding).unwrap_or_default();
                    matched |= constant_time_eq(&presented, &expected);
                }
                matched
            }
            SigningCheck::Signature { scheme, encoding } => {
                let key = secrets.public_key.as_deref().ok_or(VerifyError::NotConfigured("public key"))?;
                let verifies = public_key_verifier(*scheme, key)?;
                self.candidates.iter().filter_map(|c| decoded(c, *encoding)).any(|sig| verifies(&self.message, &sig))
            }
        };
        if matched {
            Ok(())
        } else {
            Err(VerifyError::BadSignature)
        }
    }
}

/// The check a public-key scheme runs, over the configured key: a
/// function from (message, signature bytes) to whether it verifies.
type Verifier = Box<dyn Fn(&[u8], &[u8]) -> bool>;

fn public_key_verifier(scheme: SignatureScheme, key: &str) -> Result<Verifier, VerifyError> {
    match scheme {
        SignatureScheme::Ed25519 => {
            let key_bytes: [u8; 32] = hex_to_bytes(key.trim())
                .and_then(|b| b.try_into().ok())
                .ok_or(VerifyError::NotConfigured("usable public key (ed25519 keys are 32 hex-encoded bytes)"))?;
            let verifier = ed25519_dalek::VerifyingKey::from_bytes(&key_bytes)
                .map_err(|_| VerifyError::NotConfigured("usable public key (not an ed25519 point)"))?;
            Ok(Box::new(move |message, signature| {
                let Ok(sig_bytes) = <[u8; 64]>::try_from(signature) else { return false };
                verifier.verify(message, &ed25519_dalek::Signature::from_bytes(&sig_bytes)).is_ok()
            }))
        }
        SignatureScheme::EcdsaP256 => {
            let der = base64::engine::general_purpose::STANDARD
                .decode(key.trim())
                .map_err(|_| VerifyError::NotConfigured("usable public key (ecdsa_p256 keys are base64 DER)"))?;
            let verifier = p256::ecdsa::VerifyingKey::from_public_key_der(&der)
                .map_err(|_| VerifyError::NotConfigured("usable public key (not a P-256 key)"))?;
            Ok(Box::new(move |message, signature| {
                p256::ecdsa::Signature::from_der(signature).is_ok_and(|sig| verifier.verify(message, &sig).is_ok())
            }))
        }
    }
}

/// The exact bytes a signing scheme covers: the declared `concat`
/// template resolved against the request, splicing the body as BYTES
/// (never parsed) and the other parts as text, `{timestamp}` as the
/// request wrote it. The one resolver behind both the HMAC and the
/// public-key schemes; pure and separately testable against a
/// scheme's documented example.
pub fn signed_bytes(concat: &SignedConcat, timestamp: Option<&str>, push: &PushParts<'_>) -> Result<Vec<u8>, VerifyError> {
    resolve_concat(concat, timestamp, &Request::new(push))
}

fn resolve_concat(concat: &SignedConcat, timestamp: Option<&str>, request: &Request<'_, '_>) -> Result<Vec<u8>, VerifyError> {
    let push = request.push;
    let mut out = Vec::with_capacity(concat.as_str().len() + push.body.len());
    for segment in concat.segments() {
        match segment {
            SignedSegment::Literal(text) => out.extend_from_slice(text.as_bytes()),
            SignedSegment::Part(SignedPart::Timestamp) => {
                // A concat that signs one always has a declared source
                // (refused at validation otherwise).
                let ts = timestamp.ok_or_else(|| VerifyError::Missing("timestamp".into()))?;
                out.extend_from_slice(ts.as_bytes())
            }
            SignedSegment::Part(SignedPart::Body) => out.extend_from_slice(push.body),
            SignedSegment::Part(SignedPart::Method) => out.extend_from_slice(push.method.as_bytes()),
            SignedSegment::Part(SignedPart::Url) => {
                let url = push.url.ok_or(VerifyError::NotConfigured("public receiver address"))?;
                out.extend_from_slice(url.as_bytes());
            }
            SignedSegment::Part(SignedPart::SortedFormParams) => {
                out.extend_from_slice(sorted_form_params(push.body).as_bytes())
            }
            SignedSegment::Part(SignedPart::Value(at)) => out.extend_from_slice(request.value(at)?.as_bytes()),
        }
    }
    Ok(out)
}

/// The form-encoded body's pairs, sorted by name (then value),
/// concatenated as name+value with nothing between: the rule
/// Twilio-class schemes sign POST parameters by.
fn sorted_form_params(body: &[u8]) -> String {
    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(body)
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort();
    let mut out = String::new();
    for (k, v) in pairs {
        out.push_str(&k);
        out.push_str(&v);
    }
    out
}

/// The digest an HMAC scheme expects for `push`, written in the
/// scheme's encoding, without the layout around it: what a SENDER puts
/// in the signature's slot (`v1,<this>`, `sha256=<this>`). `timestamp`
/// is the text `{timestamp}` splices. For tests and for weft's own
/// senders; the verifier compares bytes and never builds this.
pub fn hmac_digest(
    kind: &VerifyKind,
    stored_secret: &str,
    timestamp: Option<&str>,
    push: &PushParts<'_>,
) -> Result<String, VerifyError> {
    let VerifyKind::Hmac { concat, secret, algorithm, encoding, .. } = kind else {
        return Err(VerifyError::NotConfigured("hmac scheme to sign with"));
    };
    let digest = hmac_bytes(&hmac_key(secret, stored_secret)?, *algorithm, &signed_bytes(concat, timestamp, push)?);
    Ok(match encoding {
        DigestEncoding::Hex => hex_of(&digest),
        DigestEncoding::Base64 => base64::engine::general_purpose::STANDARD.encode(digest),
    })
}

/// Whether a scheme is verified against keys fetched from the issuer
/// rather than against material we already hold. Those take a
/// different (async) path; everything else is [`verify_push`].
pub fn needs_issuer_keys(kind: &VerifyKind) -> bool {
    matches!(kind, VerifyKind::Oidc { .. })
}

/// The claims a signed identity token must satisfy, checked after its
/// signature. Pure, so the policy is testable without any key at all.
pub fn check_identity_claims(
    claims: &IdentityClaims,
    secrets: &VerifySecrets,
) -> Result<(), VerifyError> {
    let expected_audience = secrets
        .audience
        .as_deref()
        .ok_or(VerifyError::NotConfigured("push audience"))?;
    if claims.audience != expected_audience {
        return Err(VerifyError::BadIdentityToken(format!(
            "it names the audience '{}', not this receiver",
            claims.audience
        )));
    }
    // An unverified address proves nothing about who is pushing.
    if !claims.email_verified {
        return Err(VerifyError::BadIdentityToken(
            "its issuing identity is not verified".into(),
        ));
    }
    if let Some(expected) = secrets.expected_email.as_deref() {
        if claims.email.as_deref() != Some(expected) {
            return Err(VerifyError::BadIdentityToken(
                "it was issued to a different identity than the one configured to push".into(),
            ));
        }
    }
    Ok(())
}

/// The claims read off a signed identity token.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IdentityClaims {
    #[serde(rename = "aud")]
    pub audience: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
    #[serde(rename = "exp")]
    pub expires_at: i64,
}

/// Constant-time byte comparison: a signature check that returns
/// early on the first differing byte leaks the correct prefix through
/// timing.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// The Authorization header's bearer value, which is where a signed
/// identity token rides.
pub fn bearer_token(headers: &[(String, String)]) -> Option<&str> {
    let (_, raw) = headers.iter().find(|(k, _)| k.eq_ignore_ascii_case("Authorization"))?;
    raw.strip_prefix("Bearer ").or_else(|| raw.strip_prefix("bearer ")).map(str::trim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::events::TIMESTAMP_TOLERANCE_SECS;
    use serde_json::json;

    const TOLERANCE: i64 = TIMESTAMP_TOLERANCE_SECS as i64;

    fn secrets(signing: &str) -> VerifySecrets {
        VerifySecrets { signing_secret: Some(signing.into()), ..Default::default() }
    }

    fn kind(v: Value) -> VerifyKind {
        let kind: VerifyKind = serde_json::from_value(v).expect("the scheme parses");
        kind.validate("svc", crate::access::events::VerifySide::Push).expect("the scheme is valid");
        kind
    }

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn parts<'a>(body: &'a [u8], headers: &'a [(String, String)]) -> PushParts<'a> {
        PushParts { body, headers, url: None, method: "POST" }
    }

    /// An HMAC-SHA256 hex digest computed straight from the hmac crate,
    /// so a provider's documented basestring is checked against the
    /// declared concat without going through it.
    fn direct_sha256_hex(key: &[u8], message: &[u8]) -> String {
        hex_of(&hmac_bytes(key, HmacAlgorithm::Sha256, message))
    }

    /// Slack's scheme as data: `v0=<hex>` in one header, the unix
    /// timestamp in another, `v0:{timestamp}:{body}` signed.
    fn slack_kind() -> VerifyKind {
        kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Slack-Signature", "pair": "=", "key": "v0" },
            "timestamp": { "in": "header:X-Slack-Request-Timestamp" },
            "concat": "v0:{timestamp}:{body}"
        }))
    }

    fn slack_signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
        let digest = hmac_digest(&slack_kind(), secret, Some(&timestamp.to_string()), &parts(body, &[])).unwrap();
        format!("v0={digest}")
    }

    /// Slack's documented worked example
    /// (https://docs.slack.dev/authentication/verifying-requests-from-slack):
    /// this exact secret, timestamp and body produce this exact
    /// signature THROUGH THE GENERIC TEMPLATE AND LAYOUT.
    #[test]
    fn the_slack_parameterization_matches_its_documented_example() {
        let secret = "8f742231b10e8888abcd99yyyzzz85a5";
        let timestamp = 1531420618;
        let body = b"token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
        let documented = "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503";
        assert_eq!(slack_signature(secret, timestamp, body), documented);
        let sent = headers(&[("X-Slack-Request-Timestamp", "1531420618"), ("X-Slack-Signature", documented)]);
        verify_push(&slack_kind(), &parts(body, &sent), &secrets(secret), timestamp).expect("the documented request verifies");
    }

    #[test]
    fn a_genuine_push_verifies_and_a_tampered_one_does_not() {
        let secret = "topsecret";
        let now = 1_700_000_000;
        let body = br#"{"event":{"type":"message"}}"#;
        let sent = headers(&[
            ("X-Slack-Request-Timestamp", &now.to_string()),
            ("X-Slack-Signature", &slack_signature(secret, now, body)),
        ]);
        verify_push(&slack_kind(), &parts(body, &sent), &secrets(secret), now).expect("a genuine push verifies");
        let tampered = br#"{"event":{"type":"messagE"}}"#;
        assert_eq!(verify_push(&slack_kind(), &parts(tampered, &sent), &secrets(secret), now), Err(VerifyError::BadSignature));
        assert_eq!(verify_push(&slack_kind(), &parts(body, &sent), &secrets("other"), now), Err(VerifyError::BadSignature));
    }

    /// A replayed push (a genuine signature from long ago) is refused
    /// on the timestamp, which is the whole reason the timestamp is
    /// inside the signed string.
    #[test]
    fn a_replayed_push_is_refused_on_its_age() {
        let secret = "topsecret";
        let signed_at = 1_700_000_000;
        let body = b"{}";
        let sent = headers(&[
            ("X-Slack-Request-Timestamp", &signed_at.to_string()),
            ("X-Slack-Signature", &slack_signature(secret, signed_at, body)),
        ]);
        assert!(matches!(
            verify_push(&slack_kind(), &parts(body, &sent), &secrets(secret), signed_at + TOLERANCE + 1),
            Err(VerifyError::StaleTimestamp(_))
        ));
        verify_push(&slack_kind(), &parts(body, &sent), &secrets(secret), signed_at + TOLERANCE).expect("inside the window");
    }

    /// Headers are read case-insensitively; a missing one is refused
    /// by name; the same header sent twice with two values is refused
    /// as ambiguous rather than one silently winning, while the same
    /// value twice is still one.
    #[test]
    fn missing_repeated_or_oddly_cased_headers_are_handled() {
        let secret = "s";
        let now = 1_700_000_000;
        let body = b"{}";
        let signature = slack_signature(secret, now, body);
        let lowercased = headers(&[("x-slack-request-timestamp", &now.to_string()), ("x-slack-signature", &signature)]);
        verify_push(&slack_kind(), &parts(body, &lowercased), &secrets(secret), now).expect("headers are case-insensitive");

        assert_eq!(
            verify_push(&slack_kind(), &parts(body, &[]), &secrets(secret), now),
            Err(VerifyError::Missing("header:X-Slack-Signature (the signature)".into()))
        );

        let twice = headers(&[
            ("x-slack-request-timestamp", &now.to_string()),
            ("X-Slack-Request-Timestamp", &(now - 1).to_string()),
            ("x-slack-signature", &signature),
        ]);
        assert!(matches!(
            verify_push(&slack_kind(), &parts(body, &twice), &secrets(secret), now),
            Err(VerifyError::Ambiguous(_))
        ));
        let same_twice = headers(&[
            ("x-slack-request-timestamp", &now.to_string()),
            ("X-Slack-Request-Timestamp", &now.to_string()),
            ("x-slack-signature", &signature),
        ]);
        verify_push(&slack_kind(), &parts(body, &same_twice), &secrets(secret), now).expect("one value sent twice is one value");
    }

    /// With no secret configured the answer is "cannot verify", never
    /// "verified", even for a genuinely signed push: an unconfigured
    /// receiver must not accept pushes.
    #[test]
    fn an_unconfigured_receiver_refuses_rather_than_accepts() {
        let now = 1_700_000_000;
        let sent = headers(&[
            ("X-Slack-Request-Timestamp", &now.to_string()),
            ("X-Slack-Signature", &slack_signature("s", now, b"{}")),
        ]);
        let err = verify_push(&slack_kind(), &parts(b"{}", &sent), &VerifySecrets::default(), now).unwrap_err();
        assert!(matches!(err, VerifyError::NotConfigured(_)), "{err}");
    }

    /// A timestamp at the edge of what the request can write is refused
    /// as stale, never an overflow: the distance is taken without
    /// subtracting the two.
    #[test]
    fn an_extreme_timestamp_is_stale_rather_than_an_overflow() {
        let secret = "s";
        for (sent, now) in [(i64::MIN, i64::MAX), (i64::MAX, i64::MIN), (i64::MIN, 1_700_000_000)] {
            let body = b"{}";
            let signed = headers(&[
                ("X-Slack-Request-Timestamp", &sent.to_string()),
                ("X-Slack-Signature", &slack_signature(secret, sent, body)),
            ]);
            assert!(
                matches!(verify_push(&slack_kind(), &parts(body, &signed), &secrets(secret), now), Err(VerifyError::StaleTimestamp(_))),
                "{sent} at {now}"
            );
        }
    }

    /// A push is read once and checked against each candidate's
    /// material: one app's missing or malformed secret is that app's
    /// failure, and the next app's secret still verifies.
    #[test]
    fn a_signed_push_is_read_once_and_checked_per_secret() {
        let now = 1_700_000_000;
        let body = b"{}";
        let sent = headers(&[
            ("X-Slack-Request-Timestamp", &now.to_string()),
            ("X-Slack-Signature", &slack_signature("right", now, body)),
        ]);
        let kind = slack_kind();
        let push = parts(body, &sent);
        let signed = SignedPush::read(&kind, &push, now).expect("the request is whole");
        assert!(matches!(signed.check(&VerifySecrets::default()), Err(VerifyError::NotConfigured(_))));
        assert_eq!(signed.check(&secrets("wrong")), Err(VerifyError::BadSignature));
        signed.check(&secrets("right")).expect("the right secret verifies");
        // A request-level fault is the read's, whatever the secrets.
        assert!(matches!(SignedPush::read(&kind, &parts(body, &[]), now), Err(VerifyError::Missing(_))));
    }

    /// GitHub's documented test values
    /// (https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries):
    /// `sha256=<hex>` over the body alone, no timestamp. An uppercase
    /// hex digest is the same bytes.
    #[test]
    fn the_github_parameterization_matches_its_documented_example() {
        let github = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Hub-Signature-256", "pair": "=", "key": "sha256" },
            "concat": "{body}"
        }));
        let secret = "It's a Secret to Everybody";
        let body = b"Hello, World!";
        let documented = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
        let sent = headers(&[("X-Hub-Signature-256", documented)]);
        verify_push(&github, &parts(body, &sent), &secrets(secret), 1_700_000_000).expect("the documented delivery verifies");
        let upper = headers(&[("X-Hub-Signature-256", &documented.replace("757107ea0eb", "757107EA0EB"))]);
        verify_push(&github, &parts(body, &upper), &secrets(secret), 0).expect("hex reads in either case");
        assert_eq!(verify_push(&github, &parts(b"Hello, World?", &sent), &secrets(secret), 0), Err(VerifyError::BadSignature));
    }

    /// Shopify's shape (https://shopify.dev/docs/apps/build/webhooks/verify-deliveries):
    /// a bare base64 HMAC-SHA256 of the raw body in
    /// `X-Shopify-Hmac-SHA256`, keyed by the app's client secret.
    #[test]
    fn the_shopify_parameterization_verifies_a_bare_base64_digest() {
        let shopify = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Shopify-Hmac-SHA256" },
            "concat": "{body}",
            "encoding": "base64"
        }));
        let secret = "shpss_client_secret";
        let body = br#"{"id":820982911946154508}"#;
        let expected = base64::engine::general_purpose::STANDARD
            .encode(hmac_bytes(secret.as_bytes(), HmacAlgorithm::Sha256, body));
        assert_eq!(hmac_digest(&shopify, secret, None, &parts(body, &[])).unwrap(), expected);
        let sent = headers(&[("x-shopify-hmac-sha256", &expected)]);
        verify_push(&shopify, &parts(body, &sent), &secrets(secret), 0).expect("base64 verifies");
        let unpadded = headers(&[("x-shopify-hmac-sha256", expected.trim_end_matches('='))]);
        verify_push(&shopify, &parts(body, &unpadded), &secrets(secret), 0).expect("padding is optional");
        assert_eq!(verify_push(&shopify, &parts(b"{}", &sent), &secrets(secret), 0), Err(VerifyError::BadSignature));
    }

    /// Stripe's layout (https://docs.stripe.com/webhooks#verify-manually):
    /// `t=<ts>,v1=<hex>` in one header, `{t}.{body}` signed. A second
    /// `v1` (a rolled secret's overlap) still verifies, a `v0` entry is
    /// ignored, and the packed timestamp is replay-checked.
    #[test]
    fn the_stripe_layout_carries_its_own_timestamp() {
        let stripe = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:Stripe-Signature", "list": ",", "pair": "=", "key": "v1" },
            "timestamp": { "in": "signature:t" },
            "concat": "{timestamp}.{body}"
        }));
        let secret = "whsec_test";
        let body = br#"{"id":"evt_1"}"#;
        let now = 1_700_000_000;
        let digest = hmac_digest(&stripe, secret, Some(&now.to_string()), &parts(body, &[])).unwrap();
        assert_eq!(digest, direct_sha256_hex(secret.as_bytes(), format!("{now}.{}", std::str::from_utf8(body).unwrap()).as_bytes()));
        let sent = headers(&[("Stripe-Signature", &format!("t={now},v1=deadbeef,v1={digest},v0={digest}"))]);
        verify_push(&stripe, &parts(body, &sent), &secrets(secret), now).expect("any matching v1 verifies");

        let stale = headers(&[("Stripe-Signature", &format!("t={},v1={digest}", now - TOLERANCE - 10))]);
        assert!(matches!(verify_push(&stripe, &parts(body, &stale), &secrets(secret), now), Err(VerifyError::StaleTimestamp(_))));
        let only_v0 = headers(&[("Stripe-Signature", &format!("t={now},v0={digest}"))]);
        assert!(matches!(verify_push(&stripe, &parts(body, &only_v0), &secrets(secret), now), Err(VerifyError::Missing(_))));
        let wrong = headers(&[("Stripe-Signature", &format!("t={now},v1=deadbeef"))]);
        assert_eq!(verify_push(&stripe, &parts(body, &wrong), &secrets(secret), now), Err(VerifyError::BadSignature));
    }

    /// Paddle's layout (https://developer.paddle.com/webhooks/signature-verification):
    /// `ts=<ts>;h1=<hex>`, `{ts}:{body}` signed, and Paddle's own
    /// default window of five seconds.
    #[test]
    fn the_paddle_layout_splits_on_semicolons() {
        let paddle = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:Paddle-Signature", "list": ";", "pair": "=", "key": "h1" },
            "timestamp": { "in": "signature:ts", "toleranceSecs": 5 },
            "concat": "{timestamp}:{body}"
        }));
        let secret = "pdl_ntfset_01gkpjp8bkm3tm53kdgkx6sms7_6h3qd3uFSi9YCD3OLYAShQI90XTI5vEI";
        let body = br#"{"event_id":"evt_01","event_type":"transaction.completed"}"#;
        let ts = 1671552777;
        let digest = hmac_digest(&paddle, secret, Some(&ts.to_string()), &parts(body, &[])).unwrap();
        assert_eq!(digest, direct_sha256_hex(secret.as_bytes(), format!("{ts}:{}", std::str::from_utf8(body).unwrap()).as_bytes()));
        let sent = headers(&[("Paddle-Signature", &format!("ts={ts};h1={digest}"))]);
        verify_push(&paddle, &parts(body, &sent), &secrets(secret), ts + 5).expect("inside Paddle's window");
        assert!(matches!(
            verify_push(&paddle, &parts(body, &sent), &secrets(secret), ts + 6),
            Err(VerifyError::StaleTimestamp(6))
        ));
    }

    fn twilio_kind() -> VerifyKind {
        kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Twilio-Signature" },
            "concat": "{url}{sorted_form_params}",
            "algorithm": "sha1",
            "encoding": "base64"
        }))
    }

    /// Twilio's own SDK test vector
    /// (https://raw.githubusercontent.com/twilio/twilio-python/main/tests/unit/test_request_validator.py):
    /// the full URL, query included, then the POST params sorted by
    /// name and appended name+value, HMAC-SHA1 under the auth token,
    /// base64 (https://www.twilio.com/docs/usage/webhooks/webhooks-security).
    #[test]
    fn the_twilio_parameterization_matches_its_sdk_vector() {
        let body = b"CallSid=CA1234567890ABCDE&Digits=1234&From=%2B14158675309&To=%2B18005551212&Caller=%2B14158675309";
        let url = "https://mycompany.com/myapp.php?foo=1&bar=2";
        let sent = headers(&[("X-Twilio-Signature", "RSOYDt4T1cUTdK1PDd93/VVr8B8=")]);
        let push = PushParts { body, headers: &sent, url: Some(url), method: "POST" };
        assert_eq!(
            String::from_utf8(signed_bytes(&"{url}{sorted_form_params}".parse().unwrap(), None, &push).unwrap()).unwrap(),
            "https://mycompany.com/myapp.php?foo=1&bar=2CallSidCA1234567890ABCDECaller+14158675309Digits1234From+14158675309To+18005551212"
        );
        verify_push(&twilio_kind(), &push, &secrets("12345"), 0).expect("the SDK's vector verifies");

        // An address-signing scheme with no known address refuses as
        // unconfigured, never verifies blind.
        let blind = PushParts { url: None, ..push };
        assert!(matches!(verify_push(&twilio_kind(), &blind, &secrets("12345"), 0), Err(VerifyError::NotConfigured(_))));
        // The query is part of what is signed.
        let no_query = PushParts { url: Some("https://mycompany.com/myapp.php"), ..push };
        assert_eq!(verify_push(&twilio_kind(), &no_query, &secrets("12345"), 0), Err(VerifyError::BadSignature));
    }

    fn standard_webhooks_kind() -> VerifyKind {
        kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:webhook-signature", "list": " ", "pair": ",", "key": "v1" },
            "timestamp": { "in": "header:webhook-timestamp" },
            "concat": "{header:webhook-id}.{timestamp}.{body}",
            "secret": { "stripPrefix": "whsec_", "decode": "base64" },
            "encoding": "base64"
        }))
    }

    /// The Standard Webhooks reference library's own vector
    /// (https://raw.githubusercontent.com/standard-webhooks/standard-webhooks/main/libraries/python/tests/test_webhooks.py,
    /// `test_sign_function`), under the spec's scheme
    /// (https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md):
    /// `{id}.{timestamp}.{body}` under the base64-decoded `whsec_` key,
    /// `v1,<base64>` entries separated by spaces.
    #[test]
    fn the_standard_webhooks_parameterization_matches_its_reference_vector() {
        let secret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
        let body = br#"{"test": 2432232314}"#;
        let ts = 1614265330;
        let base = [("webhook-id", "msg_p5jXN8AQM9LWM0D4loKWxJek"), ("webhook-timestamp", "1614265330")];
        let with = |signature: &str| {
            let mut h = headers(&base);
            h.push(("webhook-signature".into(), signature.into()));
            h
        };
        let sent = with("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
        assert_eq!(
            hmac_digest(&standard_webhooks_kind(), secret, Some("1614265330"), &parts(body, &sent)).unwrap(),
            "g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
        );
        verify_push(&standard_webhooks_kind(), &parts(body, &sent), &secrets(secret), ts).expect("the reference vector verifies");

        // Several signatures in one header (a rolled secret, or an
        // asymmetric `v1a` beside it): any matching `v1` verifies.
        let listed = with("v1a,c29tZXRoaW5nZWxzZQ== v1,bm90IGl0 v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=");
        verify_push(&standard_webhooks_kind(), &parts(body, &listed), &secrets(secret), ts).expect("one of several");
        // The library's own negative case: one character off.
        let off = with("v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OA=");
        assert_eq!(verify_push(&standard_webhooks_kind(), &parts(body, &off), &secrets(secret), ts), Err(VerifyError::BadSignature));
        // The id is signed: another one does not verify.
        let mut other_id = sent.clone();
        other_id[0].1 = "msg_other".into();
        assert_eq!(verify_push(&standard_webhooks_kind(), &parts(body, &other_id), &secrets(secret), ts), Err(VerifyError::BadSignature));
        // The secret is pasted whole; without its prefix it is refused
        // by name rather than keyed on the wrong bytes.
        assert!(matches!(
            verify_push(&standard_webhooks_kind(), &parts(body, &sent), &secrets("MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"), ts),
            Err(VerifyError::NotConfigured(_))
        ));
    }

    /// Mailgun's scheme (https://documentation.mailgun.com/docs/mailgun/user-manual/webhooks/securing-webhooks):
    /// the timestamp, the token and the hex signature sit in the JSON
    /// body's `signature` object, and `timestamp + token` (no
    /// separator) is signed. The event itself is not covered by the
    /// signature, which is the provider's design; the timestamp dates
    /// it.
    #[test]
    fn the_mailgun_parameterization_reads_the_body() {
        let mailgun = kind(json!({
            "kind": "hmac",
            "signature": { "in": "body:signature.signature" },
            "timestamp": { "in": "body:signature.timestamp" },
            "concat": "{timestamp}{body:signature.token}"
        }));
        let key = "key-mailgun-signing";
        let ts = 1770920772;
        let token = "e0b5477167110d68991efc6b9f89f0a11066af27834600e123";
        let signature = direct_sha256_hex(key.as_bytes(), format!("{ts}{token}").as_bytes());
        let body = json!({
            "signature": { "token": token, "timestamp": ts.to_string(), "signature": signature },
            "event-data": { "event": "delivered" }
        })
        .to_string();
        verify_push(&mailgun, &parts(body.as_bytes(), &[]), &secrets(key), ts).expect("Mailgun's request verifies");

        let other_token = body.replace(token, "f0b5477167110d68991efc6b9f89f0a11066af27834600e123");
        assert_eq!(verify_push(&mailgun, &parts(other_token.as_bytes(), &[]), &secrets(key), ts), Err(VerifyError::BadSignature));
        assert!(matches!(
            verify_push(&mailgun, &parts(body.as_bytes(), &[]), &secrets(key), ts + TOLERANCE + 1),
            Err(VerifyError::StaleTimestamp(_))
        ));
        assert!(matches!(
            verify_push(&mailgun, &parts(b"not json", &[]), &secrets(key), ts),
            Err(VerifyError::Missing(_))
        ));
    }

    /// Linear's scheme (https://linear.app/developers/webhooks): hex
    /// HMAC-SHA256 of the raw body in `Linear-Signature`, the
    /// timestamp a body field in milliseconds, a one-minute window.
    #[test]
    fn the_linear_parameterization_reads_a_millisecond_body_timestamp() {
        let linear = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:Linear-Signature" },
            "timestamp": { "in": "body:webhookTimestamp", "format": "milliseconds", "toleranceSecs": 60 },
            "concat": "{body}"
        }));
        let secret = "lin_wh_secret";
        let now = 1_676_056_940;
        let body = format!(r#"{{"action":"create","type":"Issue","webhookTimestamp":{}}}"#, now * 1000 + 508);
        let digest = direct_sha256_hex(secret.as_bytes(), body.as_bytes());
        let sent = headers(&[("Linear-Signature", &digest)]);
        verify_push(&linear, &parts(body.as_bytes(), &sent), &secrets(secret), now + 60).expect("inside the minute");
        assert!(matches!(
            verify_push(&linear, &parts(body.as_bytes(), &sent), &secrets(secret), now + 61),
            Err(VerifyError::StaleTimestamp(61))
        ));
    }

    /// The remaining levers: an RFC 3339 timestamp is read for the
    /// replay check and signed as written, sha512 runs, and a hex key
    /// is decoded.
    #[test]
    fn rfc3339_timestamps_sha512_and_hex_keys_run() {
        let scheme = kind(json!({
            "kind": "hmac",
            "signature": { "in": "header:X-Sig" },
            "timestamp": { "in": "header:X-Date", "format": "rfc3339" },
            "concat": "{timestamp}\n{method}\n{body}",
            "secret": { "decode": "hex" },
            "algorithm": "sha512"
        }));
        let body = b"{}";
        let date = "2023-11-14T22:13:20Z";
        let now = 1_700_000_000;
        let mut mac = <Hmac<Sha512>>::new_from_slice(&[0xab, 0xcd]).unwrap();
        mac.update(format!("{date}\nPOST\n{{}}").as_bytes());
        let expected = hex_of(&mac.finalize().into_bytes());
        let sent = headers(&[("X-Date", date), ("X-Sig", &expected)]);
        verify_push(&scheme, &parts(body, &sent), &secrets("abcd"), now).expect("sha512 over the rfc3339 text");
        let garbled = headers(&[("X-Date", "yesterday"), ("X-Sig", &expected)]);
        assert!(matches!(verify_push(&scheme, &parts(body, &garbled), &secrets("abcd"), now), Err(VerifyError::BadTimestamp(_))));
    }

    /// The public-key schemes: a genuine ed25519 signature (Discord's
    /// shape) and a genuine ECDSA P-256 one (SendGrid's shape) verify
    /// against the configured public key; a tampered body does not,
    /// and no configured key refuses rather than accepts.
    #[test]
    fn the_public_key_schemes_verify_and_refuse() {
        // One Signer import serves both crates (the shared trait).
        use ed25519_dalek::Signer as _;

        let body = br#"{"type":1}"#;
        let now = 1_700_000_000;

        // Ed25519 (Discord): hex key, hex signature over {timestamp}{body}.
        let signing = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let message = [now.to_string().as_bytes(), body.as_slice()].concat();
        let sig = signing.sign(&message);
        let discord = kind(json!({
            "kind": "signature",
            "scheme": "ed25519",
            "signature": { "in": "header:X-Signature-Ed25519" },
            "timestamp": { "in": "header:X-Signature-Timestamp" },
            "concat": "{timestamp}{body}"
        }));
        let sent = headers(&[("X-Signature-Ed25519", &hex_of(&sig.to_bytes())), ("X-Signature-Timestamp", &now.to_string())]);
        let key = VerifySecrets { public_key: Some(hex_of(signing.verifying_key().as_bytes())), ..Default::default() };
        verify_push(&discord, &parts(body, &sent), &key, now).expect("a genuine ed25519 push");
        assert_eq!(verify_push(&discord, &parts(b"{}", &sent), &key, now), Err(VerifyError::BadSignature));
        assert!(matches!(verify_push(&discord, &parts(body, &sent), &VerifySecrets::default(), now), Err(VerifyError::NotConfigured(_))));

        // ECDSA P-256 (SendGrid): base64 DER key, base64 DER signature.
        let signing = p256::ecdsa::SigningKey::from_bytes((&[9u8; 32]).into()).unwrap();
        let der_sig: p256::ecdsa::DerSignature = signing.sign(&message);
        let sendgrid = kind(json!({
            "kind": "signature",
            "scheme": "ecdsa_p256",
            "signature": { "in": "header:X-Twilio-Email-Event-Webhook-Signature" },
            "timestamp": { "in": "header:X-Twilio-Email-Event-Webhook-Timestamp" },
            "concat": "{timestamp}{body}",
            "encoding": "base64"
        }));
        let sent = headers(&[
            ("X-Twilio-Email-Event-Webhook-Signature", &base64::engine::general_purpose::STANDARD.encode(der_sig.as_bytes())),
            ("X-Twilio-Email-Event-Webhook-Timestamp", &now.to_string()),
        ]);
        let spki = p256::pkcs8::EncodePublicKey::to_public_key_der(signing.verifying_key()).unwrap();
        let key = VerifySecrets {
            public_key: Some(base64::engine::general_purpose::STANDARD.encode(spki.as_bytes())),
            ..Default::default()
        };
        verify_push(&sendgrid, &parts(body, &sent), &key, now).expect("a genuine ecdsa push");
        assert_eq!(verify_push(&sendgrid, &parts(b"{}", &sent), &key, now), Err(VerifyError::BadSignature));
    }

    /// The no-signature scheme compares the echoed token against the
    /// one weft minted, in constant time.
    #[test]
    fn the_minted_token_scheme_compares_what_was_echoed() {
        let secrets = VerifySecrets {
            presented_token: Some("echoed-token".into()),
            minted_token: Some("echoed-token".into()),
            ..Default::default()
        };
        verify_push(&VerifyKind::TokenEcho, &parts(b"", &[]), &secrets, 0).expect("the echoed token matches what we minted");

        let wrong = VerifySecrets {
            presented_token: Some("guessed".into()),
            minted_token: Some("echoed-token".into()),
            ..Default::default()
        };
        assert_eq!(verify_push(&VerifyKind::TokenEcho, &parts(b"", &[]), &wrong, 0), Err(VerifyError::BadSignature));

        // A subscription we never minted a token for cannot verify.
        let unminted = VerifySecrets { presented_token: Some("anything".into()), ..Default::default() };
        assert!(matches!(verify_push(&VerifyKind::TokenEcho, &parts(b"", &[]), &unminted, 0), Err(VerifyError::NotConfigured(_))));
    }

    /// The identity-token claim policy, independent of any signature:
    /// the audience must be ours, the identity must be verified, and
    /// (when configured) it must be the identity we expect.
    #[test]
    fn identity_claims_are_checked_against_the_configured_receiver() {
        let claims = |aud: &str, verified: bool, email: &str| IdentityClaims {
            audience: aud.into(),
            email: Some(email.into()),
            email_verified: verified,
            expires_at: 0,
        };
        let secrets = VerifySecrets {
            audience: Some("https://weft.example/events/google".into()),
            expected_email: Some("pusher@project.iam.gserviceaccount.com".into()),
            ..Default::default()
        };
        check_identity_claims(
            &claims("https://weft.example/events/google", true, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .expect("our audience, verified, the configured identity");

        let err = check_identity_claims(
            &claims("https://somebody-else/events", true, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("audience"), "{err}");

        let err = check_identity_claims(
            &claims("https://weft.example/events/google", false, "pusher@project.iam.gserviceaccount.com"),
            &secrets,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("not verified"), "{err}");

        let err = check_identity_claims(&claims("https://weft.example/events/google", true, "someone@else.com"), &secrets)
            .unwrap_err();
        assert!(format!("{err}").contains("different identity"), "{err}");
    }

    #[test]
    fn the_bearer_token_is_read_off_the_authorization_header() {
        assert_eq!(bearer_token(&headers(&[("authorization", "Bearer abc.def.ghi")])), Some("abc.def.ghi"));
        assert_eq!(bearer_token(&headers(&[("Authorization", "bearer abc")])), Some("abc"));
        assert_eq!(bearer_token(&[]), None);
        assert_eq!(bearer_token(&headers(&[("Authorization", "Basic zzz")])), None);
    }

    fn api_keys_kind() -> VerifyKind {
        VerifyKind::ApiKeys { header: "X-Api-Key".into(), bearer: true }
    }

    fn key_secrets(keys: &[&str]) -> VerifySecrets {
        VerifySecrets { api_keys: Some(keys.iter().map(|k| k.to_string()).collect()), ..Default::default() }
    }

    /// A key on its header or in the bearer slot verifies and names its
    /// position; a wrong key, a missing key, and an empty list refuse.
    #[test]
    fn a_shared_key_verifies_and_names_which_one() {
        let keys = key_secrets(&["first", "second"]);
        let on_header = headers(&[("x-api-key", "second")]);
        assert_eq!(verify_push(&api_keys_kind(), &parts(b"", &on_header), &keys, 0).unwrap(), json!({ "key": 1 }));
        let as_bearer = headers(&[("Authorization", "Bearer first")]);
        assert_eq!(verify_push(&api_keys_kind(), &parts(b"", &as_bearer), &keys, 0).unwrap(), json!({ "key": 0 }));
        let wrong = headers(&[("x-api-key", "third")]);
        assert_eq!(verify_push(&api_keys_kind(), &parts(b"", &wrong), &keys, 0), Err(VerifyError::BadSignature));
        assert!(matches!(verify_push(&api_keys_kind(), &parts(b"", &[]), &keys, 0), Err(VerifyError::Missing(_))));
        assert!(matches!(
            verify_push(&api_keys_kind(), &parts(b"", &on_header), &key_secrets(&[]), 0),
            Err(VerifyError::NotConfigured(_))
        ));
        // Bearer disabled: only the header is read.
        let no_bearer = VerifyKind::ApiKeys { header: "X-Api-Key".into(), bearer: false };
        assert!(matches!(verify_push(&no_bearer, &parts(b"", &as_bearer), &keys, 0), Err(VerifyError::Missing(_))));
    }

    /// The connection's stored values feed the schemes by fixed names,
    /// and the key list splits one per line.
    #[test]
    fn secrets_come_off_the_connection_by_fixed_names() {
        let values = BTreeMap::from([
            ("signingSecret".to_string(), "s".to_string()),
            ("keys".to_string(), " k1 , k2\nk3\n".to_string()),
            ("audience".to_string(), "aud".to_string()),
        ]);
        let secrets = secrets_from_values(&values);
        assert_eq!(secrets.signing_secret.as_deref(), Some("s"));
        assert_eq!(secrets.api_keys, Some(vec!["k1".to_string(), "k2".to_string(), "k3".to_string()]));
        assert_eq!(secrets.audience.as_deref(), Some("aud"));
        assert!(secrets.public_key.is_none());
    }

    /// The origin-proving schemes answer an empty identity; the wire
    /// callers rely on the shape being an object either way.
    #[test]
    fn origin_schemes_answer_an_anonymous_identity() {
        let secret = "topsecret";
        let now = 1_700_000_000;
        let body = b"{}";
        let sent = headers(&[
            ("X-Slack-Request-Timestamp", &now.to_string()),
            ("X-Slack-Signature", &slack_signature(secret, now, body)),
        ]);
        let identity = verify_push(&slack_kind(), &parts(body, &sent), &secrets(secret), now).unwrap();
        assert_eq!(identity, json!({}));
    }
}

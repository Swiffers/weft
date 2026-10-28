//! Signing and checking JSON Web Tokens, on ring: the one crypto library
//! every other part of weft already runs on (`crate::net`'s TLS).
//!
//! `jsonwebtoken` carries no crypto of its own and offers two backends:
//! RustCrypto's, which pulls the `rsa` crate (RUSTSEC-2023-0071, a timing
//! leak with no fixed release), and aws-lc-rs, a second crypto library
//! that compiles a whole C library into the build. So weft hands it ring
//! itself. Every token weft signs or checks goes
//! through [`encode`] and [`decode`] here, which install this provider
//! before the first use: `jsonwebtoken` settles its provider on first
//! use, and one settled before the install would be its panicking
//! placeholder.
//!
//! Only the algorithms weft uses are served: RS256 (a connection's minted
//! token, Google's identity tokens, a gated route's callers) and HS256.
//! Any other fails as an invalid algorithm.

use jsonwebtoken::crypto::{CryptoProvider, JwtSigner, JwtVerifier, KeyUtils};
use jsonwebtoken::errors::{ErrorKind, Result};
use jsonwebtoken::signature::{Error as SignatureError, Signer, Verifier};
use jsonwebtoken::{AlgorithmFamily, DecodingKeyKind};
use ring::signature::{RsaKeyPair, RsaPublicKeyComponents, UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256};

pub use jsonwebtoken::errors;
pub use jsonwebtoken::jwk;
pub use jsonwebtoken::{decode_header, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation};

/// `claims` signed with `key` under `header`'s algorithm.
pub fn encode<T: serde::Serialize>(header: &Header, claims: &T, key: &EncodingKey) -> Result<String> {
    install();
    jsonwebtoken::encode(header, claims, key)
}

/// `token`'s claims, once its signature checks out against `key` and its
/// claims pass `validation`.
pub fn decode<T: serde::de::DeserializeOwned>(token: &str, key: &DecodingKey, validation: &Validation) -> Result<TokenData<T>> {
    install();
    jsonwebtoken::decode(token, key, validation)
}

fn install() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        // Only this module installs one, and only here, once.
        let _ = RING.install_default();
    });
}

static RING: CryptoProvider = CryptoProvider {
    signer_factory: signer,
    verifier_factory: verifier,
    key_utils: KeyUtils::new_unimplemented(),
};

fn signer(algorithm: &Algorithm, key: &EncodingKey) -> Result<Box<dyn JwtSigner>> {
    match algorithm {
        Algorithm::RS256 if key.family() == AlgorithmFamily::Rsa => {
            let pair = RsaKeyPair::from_der(key.as_bytes()).map_err(|e| ErrorKind::InvalidRsaKey(e.to_string()))?;
            Ok(Box::new(Rs256Signer(pair)))
        }
        Algorithm::HS256 if key.family() == AlgorithmFamily::Hmac => {
            Ok(Box::new(Hs256(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key.as_bytes()))))
        }
        Algorithm::RS256 | Algorithm::HS256 => Err(ErrorKind::InvalidKeyFormat.into()),
        _ => Err(ErrorKind::InvalidAlgorithm.into()),
    }
}

fn verifier(algorithm: &Algorithm, key: &DecodingKey) -> Result<Box<dyn JwtVerifier>> {
    match (algorithm, key.kind()) {
        (Algorithm::RS256, _) if key.family() == AlgorithmFamily::Rsa => Ok(Box::new(Rs256Verifier(key.clone()))),
        (Algorithm::HS256, DecodingKeyKind::SecretOrDer(secret)) if key.family() == AlgorithmFamily::Hmac => {
            Ok(Box::new(Hs256(ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret))))
        }
        (Algorithm::RS256 | Algorithm::HS256, _) => Err(ErrorKind::InvalidKeyFormat.into()),
        _ => Err(ErrorKind::InvalidAlgorithm.into()),
    }
}

struct Rs256Signer(RsaKeyPair);

impl Signer<Vec<u8>> for Rs256Signer {
    fn try_sign(&self, message: &[u8]) -> std::result::Result<Vec<u8>, SignatureError> {
        let mut signature = vec![0; self.0.public().modulus_len()];
        self.0
            .sign(&RSA_PKCS1_SHA256, &ring::rand::SystemRandom::new(), message, &mut signature)
            .map_err(|_| SignatureError::new())?;
        Ok(signature)
    }
}

impl JwtSigner for Rs256Signer {
    fn algorithm(&self) -> Algorithm {
        Algorithm::RS256
    }
}

struct Rs256Verifier(DecodingKey);

impl Verifier<Vec<u8>> for Rs256Verifier {
    fn verify(&self, message: &[u8], signature: &Vec<u8>) -> std::result::Result<(), SignatureError> {
        let checked = match self.0.kind() {
            DecodingKeyKind::SecretOrDer(der) => UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, der).verify(message, signature),
            DecodingKeyKind::RsaModulusExponent { n, e } => {
                RsaPublicKeyComponents { n, e }.verify(&RSA_PKCS1_2048_8192_SHA256, message, signature)
            }
        };
        checked.map_err(|_| SignatureError::new())
    }
}

impl JwtVerifier for Rs256Verifier {
    fn algorithm(&self) -> Algorithm {
        Algorithm::RS256
    }
}

struct Hs256(ring::hmac::Key);

impl Signer<Vec<u8>> for Hs256 {
    fn try_sign(&self, message: &[u8]) -> std::result::Result<Vec<u8>, SignatureError> {
        Ok(ring::hmac::sign(&self.0, message).as_ref().to_vec())
    }
}

impl Verifier<Vec<u8>> for Hs256 {
    fn verify(&self, message: &[u8], signature: &Vec<u8>) -> std::result::Result<(), SignatureError> {
        ring::hmac::verify(&self.0, message, signature).map_err(|_| SignatureError::new())
    }
}

impl JwtSigner for Hs256 {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

impl JwtVerifier for Hs256 {
    fn algorithm(&self) -> Algorithm {
        Algorithm::HS256
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key made for this test alone (`openssl genpkey -algorithm RSA`),
    /// in the PKCS#8 form a pasted service-account key takes.
    const PRIVATE: &str = "
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDHruP20Z6mZKlV
Lt+T2v7lDJur8Z66HMvvIIEeFPQDsG8NGlzSPrWKog8F93YTvLB1+bKqNB1ftI8u
UXRi/4ypUWnpsApXgEGiNCKegCi5s3idd4ooENtlwdXIPNeC3Fh0tQVSz1c7hQR4
RTDUTI41nJyota5HfvdwPO4W1C5V4ArxQddNeLrLuscLBCzh27OIMt8eq5O6P6D6
J0UB1g2FmDjvRzvPfRm1cEDQ5wzOn/XKQPd3Ek9dYP72/DjtEMdoNdrFygTpiWrx
zb4jifMrJDAU8z8fisMCccsUYXDw39ZzhsSlG2++1+TzhpkDAvh3EwD33w71YOI1
H6Yqyi01AgMBAAECggEAAa1j59JZ8U7eJ0OGCEFqK7Vf52FXsnuEcQVGYGDPrepA
/Iywo+l37+qtkqDCAqdumgcXa5uVdZTYMQzpG93GqjNvXtGmPJbstjaW5drwvKKX
zEbRR8XgeenXaF+udcBxE8Xjfg8UzgCviW0XXGkqNqQSos4n4RzdajlwzItKuljs
H96A+u8i3Hc87hr6Tovd88IuWiJV7c1rR+CrQgOCIoyd5vbA7kOFl528BYBVKkRv
HAbgJmIvLCGuzxXlrfYmMpe6skSlCufLgojS2vEoQy0hl1F+hCojQuXXWTAwrTFj
ih1feJidwn/1DXksOAhu0Z95nMiD9BcmSBAtk/BwfwKBgQDyYp8XLWQhAa+u3JW/
GOg6ZEAYSUsv4YYv0X9ARpsZBPnSCq2OWixmKiw3je+7Rb4HIyGwBk39afPRjidc
ZpIAS5vcL21C5dD+eXwAmmV6p81fxRdMA3KpYbTc5J89GuVwL1ELX77/PV5jlzB5
Ih6zehB1SZi62VGsm8FsDx5lnwKBgQDS5jwMa1yLF+PZAodZT/ZDmngwe045dufo
EakeOSFPKCVIoN2fCMCbw2H3exH6/gZDEwQ5mf/RdIpFAFYEWe6LPh4pXxjzhGCS
cwyAfjVZbu1cE9SWB4+l0YTHk0HqpdEi1c422Am7FuGl5eGLcA0HWHwKvzBcqdig
sKNzfCg0qwKBgQCT3iBp9lIIZMv1pDJik2JRBQmpB0WsHyxKjcbQwoIqAEQbpFH+
7vn5Fa9F8PwYRlLIbeFGToJTn8iTxeIzV4uoTHf4GqLQVf1q0VgZ0hcofagnzcgJ
omzX8nhw7bRLhdYE24jYpH+TnK+xl/GFZJBQM55Ivy5ChwNxhs+4CX0kxwKBgFu2
q95qwe/rD7bLlriMQXfcsO/Eb5wlzWvqYjU2evnFZ5FSA46pa7lRGkVZS9Ruq6J3
npq+EbWwjIC9SKrTwRzkgpFTPbKImsaVKZanS+P8jVRBe21TFHDz0F5J95xnR69y
cx7+jBzEMpTGh3Lv43qSTD8QLaPeaMpYhnwuPf3JAoGBAJSDGOYQTpDHZtjXH7/2
ZiKwA9qgYo29EwkYmp8fjKnnPdwfLUCf+ZIL87Xrr+Yuxv6cnFm9SLlRwKfpIf2y
zvkysf5h64ylpEHzS0zLnv7x+M87rujcZco1hAynLyMz+7IcF+ElIp20QHOJpXkb
u91aL9s1claZDZBX7i1dW3GE
-----END PRIVATE KEY-----
";
    /// The same key's public half, as a key set publishes it (`n`, `e`).
    const MODULUS: &str = "x67j9tGepmSpVS7fk9r-5Qybq_GeuhzL7yCBHhT0A7BvDRpc0j61iqIPBfd2E7ywdfmyqjQdX7SPLlF0Yv-MqVFp6bAKV4BBojQinoAoubN4nXeKKBDbZcHVyDzXgtxYdLUFUs9XO4UEeEUw1EyONZycqLWuR373cDzuFtQuVeAK8UHXTXi6y7rHCwQs4duziDLfHquTuj-g-idFAdYNhZg470c7z30ZtXBA0OcMzp_1ykD3dxJPXWD-9vw47RDHaDXaxcoE6Ylq8c2-I4nzKyQwFPM_H4rDAnHLFGFw8N_Wc4bEpRtvvtfk84aZAwL4dxMA998O9WDiNR-mKsotNQ";
    const EXPONENT: &str = "AQAB";
    /// The same key's public half as PEM.
    const PUBLIC: &str = "
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx67j9tGepmSpVS7fk9r+
5Qybq/GeuhzL7yCBHhT0A7BvDRpc0j61iqIPBfd2E7ywdfmyqjQdX7SPLlF0Yv+M
qVFp6bAKV4BBojQinoAoubN4nXeKKBDbZcHVyDzXgtxYdLUFUs9XO4UEeEUw1EyO
NZycqLWuR373cDzuFtQuVeAK8UHXTXi6y7rHCwQs4duziDLfHquTuj+g+idFAdYN
hZg470c7z30ZtXBA0OcMzp/1ykD3dxJPXWD+9vw47RDHaDXaxcoE6Ylq8c2+I4nz
KyQwFPM/H4rDAnHLFGFw8N/Wc4bEpRtvvtfk84aZAwL4dxMA998O9WDiNR+mKsot
NQIDAQAB
-----END PUBLIC KEY-----
";

    fn claims() -> serde_json::Value {
        serde_json::json!({ "sub": "x", "exp": 4_000_000_000u64 })
    }

    /// A token signed RS256 with a PKCS#8 key checks out against the key's
    /// public half, whether it comes as PEM or as a key set's `n` and `e`;
    /// one signed by another key, or altered, does not.
    #[test]
    fn an_rs256_token_checks_out_against_its_public_key_only() {
        let key = EncodingKey::from_rsa_pem(PRIVATE.trim().as_bytes()).unwrap();
        let token = encode(&Header::new(Algorithm::RS256), &claims(), &key).unwrap();
        let validation = Validation::new(Algorithm::RS256);
        for public in [DecodingKey::from_rsa_pem(PUBLIC.trim().as_bytes()).unwrap(), DecodingKey::from_rsa_components(MODULUS, EXPONENT).unwrap()] {
            let data = decode::<serde_json::Value>(&token, &public, &validation).unwrap();
            assert_eq!(data.claims["sub"], "x");
        }
        let public = DecodingKey::from_rsa_components(MODULUS, EXPONENT).unwrap();
        let (head, _) = token.rsplit_once('.').unwrap();
        let forged = format!("{head}.{}", "A".repeat(342));
        let refused = decode::<serde_json::Value>(&forged, &public, &validation).unwrap_err();
        assert_eq!(*refused.kind(), errors::ErrorKind::InvalidSignature, "{refused}");
    }

    /// HS256 signs and checks with one shared secret, and refuses another.
    #[test]
    fn an_hs256_token_checks_out_with_its_secret_only() {
        let token = encode(&Header::default(), &claims(), &EncodingKey::from_secret(b"k")).unwrap();
        let validation = Validation::new(Algorithm::HS256);
        decode::<serde_json::Value>(&token, &DecodingKey::from_secret(b"k"), &validation).unwrap();
        let refused = decode::<serde_json::Value>(&token, &DecodingKey::from_secret(b"other"), &validation).unwrap_err();
        assert_eq!(*refused.kind(), errors::ErrorKind::InvalidSignature, "{refused}");
    }

    /// An algorithm weft does not serve fails as an invalid algorithm.
    #[test]
    fn an_algorithm_weft_does_not_serve_is_refused() {
        let refused = encode(&Header::new(Algorithm::HS512), &claims(), &EncodingKey::from_secret(b"k")).unwrap_err();
        assert_eq!(*refused.kind(), errors::ErrorKind::InvalidAlgorithm, "{refused}");
    }
}

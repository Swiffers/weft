//! Sealing a batch of connections to another install (`weft migrate
//! connections`).
//!
//! The sealing in `crypt.rs` is symmetric, under one key per install, so
//! it cannot hand a secret to a second install. Here the install the
//! connections move TO makes a key pair and keeps its private half; the
//! install they move FROM seals the batch to the public half; nothing on
//! the way (the CLI, the dispatchers in between, a log) can open it.
//!
//! Sealed format: base64(ephemeral public key (65 bytes, uncompressed
//! SEC1) || nonce (12 bytes) || AES-256-GCM ciphertext), the AES key
//! drawn from the ECDH secret between the ephemeral key and the
//! recipient's with HKDF-SHA256, salted with both public keys.

use aes_gcm::aead::{Aead, Generate, KeyInit, Nonce};
use aes_gcm::Aes256Gcm;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use p256::elliptic_curve::sec1::ToSec1Point as _;
use p256::{PublicKey, SecretKey};

const POINT_SIZE: usize = 65;
const NONCE_SIZE: usize = 12;
const INFO: &[u8] = b"weft connections transfer v1";

/// A fresh key pair: the secret half as bytes (to be kept sealed), and
/// the public half as the text a source seals to.
pub fn key_pair() -> (Vec<u8>, String) {
    let secret = SecretKey::generate();
    let public = BASE64.encode(secret.public_key().to_sec1_point(false).as_bytes());
    (secret.to_bytes().to_vec(), public)
}

/// The AES key two sides agree on for one sealed batch.
fn shared_cipher(secret: &p256::NonZeroScalar, public: &PublicKey, ephemeral: &[u8], recipient: &[u8]) -> Aes256Gcm {
    let shared = p256::ecdh::diffie_hellman(secret, public.as_affine());
    let mut salt = Vec::with_capacity(ephemeral.len() + recipient.len());
    salt.extend_from_slice(ephemeral);
    salt.extend_from_slice(recipient);
    let mut key = [0u8; 32];
    shared.extract::<sha2::Sha256>(Some(&salt)).expand(INFO, &mut key).expect("32 bytes is a valid HKDF-SHA256 length");
    Aes256Gcm::new_from_slice(&key).expect("a 32-byte key always fits AES-256")
}

/// Seal `plain` so only the holder of `public`'s secret half opens it.
pub fn seal_to(public: &str, plain: &[u8]) -> anyhow::Result<String> {
    let recipient = BASE64.decode(public.trim()).map_err(|e| anyhow::anyhow!("the target install's key is not base64: {e}"))?;
    let recipient_key =
        PublicKey::from_sec1_bytes(&recipient).map_err(|_| anyhow::anyhow!("the target install's key is not a P-256 public key"))?;
    let ephemeral = SecretKey::generate();
    let ephemeral_public = ephemeral.public_key().to_sec1_point(false);
    let cipher = shared_cipher(&ephemeral.to_nonzero_scalar(), &recipient_key, ephemeral_public.as_bytes(), &recipient);
    let nonce = Nonce::<Aes256Gcm>::generate();
    let ct = cipher.encrypt(&nonce, plain).expect("AES-GCM encryption cannot fail");
    let mut out = Vec::with_capacity(POINT_SIZE + NONCE_SIZE + ct.len());
    out.extend_from_slice(ephemeral_public.as_bytes());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(BASE64.encode(out))
}

/// Open what [`seal_to`] sealed to the public half of `secret`. Loud when
/// it was sealed to another key or changed on the way.
pub fn open_with(secret: &[u8], sealed: &str) -> anyhow::Result<Vec<u8>> {
    let secret = SecretKey::from_slice(secret).map_err(|_| anyhow::anyhow!("the kept transfer key does not read"))?;
    let combined = BASE64.decode(sealed.trim()).map_err(|e| anyhow::anyhow!("the sealed connections are not base64: {e}"))?;
    anyhow::ensure!(combined.len() > POINT_SIZE + NONCE_SIZE, "the sealed connections are too short to be any");
    let (ephemeral, rest) = combined.split_at(POINT_SIZE);
    let (nonce, ct) = rest.split_at(NONCE_SIZE);
    let ephemeral_key =
        PublicKey::from_sec1_bytes(ephemeral).map_err(|_| anyhow::anyhow!("the sealed connections name no P-256 key"))?;
    let recipient = secret.public_key().to_sec1_point(false);
    let cipher = shared_cipher(&secret.to_nonzero_scalar(), &ephemeral_key, ephemeral, recipient.as_bytes());
    let nonce = Nonce::<Aes256Gcm>::try_from(nonce).expect("split at the nonce's size");
    cipher.decrypt(&nonce, ct).map_err(|_| {
        anyhow::anyhow!("the sealed connections do not open with this install's transfer key: they were sealed to another one, or changed on the way")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_is_sealed_to_a_key_opens_with_its_secret_half_only() {
        let (secret, public) = key_pair();
        let sealed = seal_to(&public, b"the refresh token").unwrap();
        assert_eq!(open_with(&secret, &sealed).unwrap(), b"the refresh token");
        assert_ne!(seal_to(&public, b"the refresh token").unwrap(), sealed, "a fresh ephemeral key and nonce every time");

        let (other, _) = key_pair();
        let refused = open_with(&other, &sealed).unwrap_err().to_string();
        assert!(refused.contains("another one"), "{refused}");

        let mut bytes = BASE64.decode(&sealed).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        assert!(open_with(&secret, &BASE64.encode(bytes)).is_err(), "a changed batch does not open");
        assert!(seal_to("not a key", b"x").is_err());
    }
}

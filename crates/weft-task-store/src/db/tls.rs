//! How weft's own connections to the install's database are encrypted.
//!
//! An address's `sslmode` says whether they are: `disable` never, `prefer`
//! (an address that names none) when the server offers it, `require`
//! always. Encrypted the way libpq encrypts those modes: the server's
//! certificate is not checked. A database on a private network presents
//! one signed by its provider's own authority (Cloud SQL's private address
//! does), which no public list of authorities holds.

use std::sync::{Arc, OnceLock};

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_postgres_rustls::MakeRustlsConnect;

/// The TLS every connection to the install's database is made with, on
/// ring like every other TLS client weft makes.
pub(crate) fn connector() -> MakeRustlsConnect {
    static CONNECTOR: OnceLock<MakeRustlsConnect> = OnceLock::new();
    CONNECTOR
        .get_or_init(|| {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .expect("ring supports rustls's default protocol versions")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(Unchecked { provider }))
                .with_no_client_auth();
            MakeRustlsConnect::new(config)
        })
        .clone()
}

/// Accepts whatever certificate the server presents (the module docs say
/// why), and still checks that the server holds its key: the handshake's
/// signatures are verified as usual.
#[derive(Debug)]
struct Unchecked {
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Unchecked {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

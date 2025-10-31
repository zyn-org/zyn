// SPDX-License-Identifier: AGPL-3.0

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::anyhow;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

/// A unified stream type that can represent either TCP or Unix domain socket connections.
///
/// This enum abstracts over the underlying transport mechanism, allowing applications
/// to work with both network and local socket connections transparently.
///
/// The implementation delegates all I/O operations to the underlying stream type,
/// maintaining their respective characteristics and behaviors.
#[derive(Debug)]
pub enum Stream {
  /// A TCP network connection.
  ///
  /// Used for remote connections over IP networks.
  Tcp(TcpStream),

  /// A Unix domain socket connection.
  ///
  /// Used for high-performance local inter-process communication.
  Unix(UnixStream),
}

// ===== impl Stream =====

impl AsyncRead for Stream {
  fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp(stream) => Pin::new(stream).poll_read(cx, buf),
      Stream::Unix(stream) => Pin::new(stream).poll_read(cx, buf),
    }
  }
}

impl AsyncWrite for Stream {
  fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
    match self.get_mut() {
      Stream::Tcp(stream) => Pin::new(stream).poll_write(cx, buf),
      Stream::Unix(stream) => Pin::new(stream).poll_write(cx, buf),
    }
  }

  fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp(stream) => Pin::new(stream).poll_flush(cx),
      Stream::Unix(stream) => Pin::new(stream).poll_flush(cx),
    }
  }

  fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    match self.get_mut() {
      Stream::Tcp(stream) => Pin::new(stream).poll_shutdown(cx),
      Stream::Unix(stream) => Pin::new(stream).poll_shutdown(cx),
    }
  }
}

/// A trait for establishing network connections and returning a stream.
///
/// The `Dialer` trait abstracts the connection logic, allowing different implementations
/// for various transport types (TCP, Unix sockets, etc.) or custom connection strategies.
#[async_trait::async_trait]
pub trait Dialer: Send + Sync + 'static {
  /// The type of stream returned by this dialer.
  ///
  /// Must implement `AsyncRead + AsyncWrite` for bidirectional I/O operations.
  type Stream: AsyncRead + AsyncWrite + Send + Sync + 'static;

  /// Establishes a connection and returns a stream.
  ///
  /// # Returns
  ///
  /// Returns a `Stream` on successful connection, or an error if the connection fails.
  async fn dial(&self) -> anyhow::Result<Self::Stream>;
}

/// TCP dialer implementation.
#[derive(Clone, Debug)]
pub struct TcpDialer {
  address: String,
}

impl TcpDialer {
  /// Creates a new TCP dialer with the specified address.
  pub fn new(address: String) -> Self {
    Self { address }
  }
}

// ===== impl TcpDialer =====

#[async_trait::async_trait]
impl Dialer for TcpDialer {
  type Stream = Stream;

  async fn dial(&self) -> anyhow::Result<Stream> {
    let tcp_stream =
      TcpStream::connect(&self.address).await.map_err(|e| anyhow!("failed to connect to {}: {}", self.address, e))?;

    tcp_stream.set_nodelay(true)?;

    Ok(Stream::Tcp(tcp_stream))
  }
}

/// Unix domain socket dialer implementation.
#[derive(Clone, Debug)]
pub struct UnixDialer {
  socket_path: String,
}

// ===== impl UnixDialer =====

impl UnixDialer {
  /// Creates a new Unix dialer with the specified socket path.
  pub fn new(socket_path: String) -> Self {
    Self { socket_path }
  }
}

#[async_trait::async_trait]
impl Dialer for UnixDialer {
  type Stream = Stream;

  async fn dial(&self) -> anyhow::Result<Stream> {
    let unix_stream = UnixStream::connect(&self.socket_path)
      .await
      .map_err(|e| anyhow!("failed to connect to {}: {}", self.socket_path, e))?;

    Ok(Stream::Unix(unix_stream))
  }
}

/// TLS dialer implementation.
#[derive(Clone)]
pub struct TlsDialer {
  address: String,
  tls_connector: TlsConnector,
  server_name: rustls::pki_types::ServerName<'static>,
}

// ===== impl TlsDialer =====

impl TlsDialer {
  /// Creates a new TLS dialer with the specified address.
  pub fn new(address: String) -> anyhow::Result<Self> {
    // Create a default TLS client configuration that accepts invalid certificates.
    let tls_config =
      ClientConfig::builder().dangerous().with_custom_certificate_verifier(Arc::new(NoVerifier)).with_no_client_auth();

    let tls_connector = TlsConnector::from(Arc::new(tls_config));

    // Extract hostname for SNI
    let server_name = address.split(':').next().ok_or_else(|| anyhow!("invalid address format"))?.to_string();

    let server_name =
      rustls::pki_types::ServerName::try_from(server_name).map_err(|_| anyhow!("invalid server name"))?.to_owned();

    Ok(Self { address, tls_connector, server_name })
  }
}

#[async_trait::async_trait]
impl Dialer for TlsDialer {
  type Stream = TlsStream<TcpStream>;

  async fn dial(&self) -> anyhow::Result<TlsStream<TcpStream>> {
    let tcp_stream =
      TcpStream::connect(&self.address).await.map_err(|e| anyhow!("failed to connect to {}: {}", self.address, e))?;

    tcp_stream.set_nodelay(true)?;

    let tls_stream = self
      .tls_connector
      .connect(self.server_name.clone(), tcp_stream)
      .await
      .map_err(|e| anyhow!("TLS handshake failed: {}", e))?;

    Ok(tls_stream)
  }
}

/// Certificate verifier that accepts all certificates (for testing/development).
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
  fn verify_server_cert(
    &self,
    _end_entity: &rustls::pki_types::CertificateDer<'_>,
    _intermediates: &[rustls::pki_types::CertificateDer<'_>],
    _server_name: &rustls::pki_types::ServerName<'_>,
    _ocsp_response: &[u8],
    _now: rustls::pki_types::UnixTime,
  ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
    Ok(rustls::client::danger::ServerCertVerified::assertion())
  }

  fn verify_tls12_signature(
    &self,
    _message: &[u8],
    _cert: &rustls::pki_types::CertificateDer<'_>,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }

  fn verify_tls13_signature(
    &self,
    _message: &[u8],
    _cert: &rustls::pki_types::CertificateDer<'_>,
    _dss: &rustls::DigitallySignedStruct,
  ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
    Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
  }

  fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
    vec![
      rustls::SignatureScheme::RSA_PKCS1_SHA256,
      rustls::SignatureScheme::RSA_PKCS1_SHA384,
      rustls::SignatureScheme::RSA_PKCS1_SHA512,
      rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
      rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
      rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
      rustls::SignatureScheme::RSA_PSS_SHA256,
      rustls::SignatureScheme::RSA_PSS_SHA384,
      rustls::SignatureScheme::RSA_PSS_SHA512,
      rustls::SignatureScheme::ED25519,
    ]
  }
}

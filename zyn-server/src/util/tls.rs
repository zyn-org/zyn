// SPDX-License-Identifier: AGPL-3.0

use std::io::{BufReader, Cursor};
use std::sync::Arc;
use std::{fs, io};

use anyhow::anyhow;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Generates a self-signed certificate and private key.
///
/// This function takes a list of alternative names (`alt_names`) for the certificate,
/// generates a self-signed certificate, and returns both the certificate and the private key.
///
/// # Arguments
///
/// * `alt_names` - A `Vec<String>` containing the alternative names for the certificate.
///
/// # Returns
///
/// * `Ok((Vec<CertificateDer<'static>>, PrivateKeyDer<'static>))` - On success, returns a tuple containing the certificate and private key.
/// * `Err(anyhow::Error)` - If an error occurs during the certificate generation or parsing.
pub fn generate_self_signed_cert(
  alt_names: Vec<String>,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
  let cert = rcgen::generate_simple_self_signed(alt_names)?;

  // Load the certificate and private key
  let cert_pem = cert.cert.pem();
  let mut cert_rd = Cursor::new(cert_pem.as_bytes());
  let mut key_rd = Cursor::new(cert.signing_key.serialize_pem());

  let certs = load_certs_from_reader(&mut cert_rd);
  let key = load_key_from_reader(&mut key_rd)?;

  Ok((certs, key))
}

/// Creates a TLS server configuration using the provided certificate and private key.
///
/// This function takes a list of certificates and a private key and creates a `ServerConfig`
/// for TLS, which is then wrapped in an `Arc` for shared ownership.
///
/// # Arguments
///
/// * `certs` - A `Vec<CertificateDer<'static>>` containing the server's certificate chain.
/// * `key` - A `PrivateKeyDer<'static>` containing the server's private key.
///
/// # Returns
///
/// * `Ok(Arc<ServerConfig>)` - On success, returns a `ServerConfig` wrapped in an `Arc`.
/// * `Err(anyhow::Error)` - If an error occurs during the configuration creation.
pub fn create_tls_config(
  certs: Vec<CertificateDer<'static>>,
  key: PrivateKeyDer<'static>,
) -> anyhow::Result<Arc<ServerConfig>> {
  let config = ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key)?;
  Ok(Arc::new(config))
}

/// Loads a list of certificates from a PEM-encoded file.
///
/// This function reads the specified file and parses it as a list of PEM-encoded certificates.
///
/// # Arguments
///
/// * `filename` - A `&str` representing the path to the certificate file.
///
/// # Returns
///
/// * `Ok(Vec<CertificateDer<'static>>)` - On success, returns a vector of `CertificateDer`.
/// * `Err(anyhow::Error)` - If an error occurs while opening or reading the file.
pub fn load_certs(filename: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
  let cert_file = fs::File::open(filename)?;
  let mut reader = BufReader::new(cert_file);
  Ok(load_certs_from_reader(&mut reader))
}

/// Loads a private key from a PEM-encoded file.
///
/// This function reads the specified file and parses it as a PEM-encoded private key.
///
/// # Arguments
///
/// * `filename` - A `&str` representing the path to the private key file.
///
/// # Returns
///
/// * `Ok(PrivateKeyDer<'static>)` - On success, returns a `PrivateKeyDer`.
/// * `Err(anyhow::Error)` - If an error occurs while opening, reading, or parsing the file.
pub fn load_private_key(filename: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
  let keyfile = fs::File::open(filename)?;
  let mut reader = BufReader::new(keyfile);
  load_key_from_reader(&mut reader)
}

fn load_certs_from_reader(rd: &mut dyn io::BufRead) -> Vec<CertificateDer<'static>> {
  rustls_pemfile::certs(rd).map(|result| result.unwrap()).collect()
}

fn load_key_from_reader(rd: &mut dyn io::BufRead) -> anyhow::Result<PrivateKeyDer<'static>> {
  loop {
    match rustls_pemfile::read_one(rd)? {
      Some(rustls_pemfile::Item::Pkcs1Key(key)) => return Ok(key.into()),
      Some(rustls_pemfile::Item::Pkcs8Key(key)) => return Ok(key.into()),
      Some(rustls_pemfile::Item::Sec1Key(key)) => return Ok(key.into()),
      None => break,
      _ => {},
    }
  }
  Err(anyhow!("no keys found in reader (encrypted keys not supported)"))
}

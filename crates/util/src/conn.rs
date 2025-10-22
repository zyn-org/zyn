// SPDX-License-Identifier: AGPL-3.0

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpStream, UnixStream};

/// A unified stream type that can represent either TCP or Unix domain socket connections.
///
/// This enum abstracts over the underlying transport mechanism, allowing the modulator
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

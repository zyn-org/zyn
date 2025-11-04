// SPDX-License-Identifier: AGPL-3.0

use std::io::Cursor;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use zyn_util::codec::StreamReader;
use zyn_util::pool::MutablePoolBuffer;

use crate::{Message, deserialize, serialize};

/// Sends a request message and returns the response.
///
/// This function implements a request-response pattern: it serializes a request,
/// writes it to the stream, reads the response, and deserializes it.
///
/// # Type Parameters
/// * `S` - The stream type (must implement AsyncRead + AsyncWrite + Unpin)
///
/// # Arguments
/// * `message` - The message to send
/// * `stream` - The stream to write to and read from
/// * `pool_buffer` - Pre-allocated buffer from a pool
///
/// # Returns
/// * `Ok(Message)` - The deserialized response message
/// * `Err(_)` - Serialization, IO, or deserialization error
///
/// # Example
/// ```no_run
/// use zyn_protocol::{Message, ConnectParameters, request};
/// use zyn_util::pool::Pool;
/// use tokio::net::TcpStream;
///
/// # async fn example(stream: &mut TcpStream) -> anyhow::Result<()> {
/// let pool = Pool::new(1, 4096);
/// let buffer = pool.must_acquire();
///
/// let connect_msg = Message::Connect(ConnectParameters {
///   protocol_version: 1,
///   heartbeat_interval: 30000,
/// });
///
/// let response = request(connect_msg, stream, buffer).await?;
/// # Ok(())
/// # }
/// ```
pub async fn request<S>(message: Message, stream: &mut S, mut pool_buffer: MutablePoolBuffer) -> anyhow::Result<Message>
where
  S: AsyncRead + AsyncWrite + Unpin,
{
  // Serialize the message into the buffer
  let n = serialize(&message, pool_buffer.as_mut_slice())?;

  // Write the message to the stream
  stream.write_all(&pool_buffer.as_slice()[..n]).await?;
  stream.flush().await?;

  // Convert to reader for reading the response
  let mut stream_reader = StreamReader::with_pool_buffer(&mut *stream, pool_buffer);

  // Read the response
  let _ = stream_reader.next().await?;

  // Deserialize the response
  match stream_reader.get_line() {
    Some(line_bytes) => {
      let response = deserialize(Cursor::new(line_bytes))?;
      Ok(response)
    },
    None => Err(anyhow::anyhow!("failed to read response from server")),
  }
}

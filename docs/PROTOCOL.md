# Zyn - Protocol Specification

## Table of Contents

- [Overview](#overview)
  - [What is a Modulator?](#what-is-a-modulator)
  - [Why a custom protocol?](#why-a-custom-protocol)
- [Preliminaries](#preliminaries)
  - [Network](#network)
  - [Connection Management](#connection-management)
  - [Message Ordering and Flow Control](#message-ordering-and-flow-control)
  - [Versioning and Compatibility](#versioning-and-compatibility)
- [Protocol](#protocol)
  - [Identifiers](#identifiers)
  - [Message Structure](#message-structure)
  - [Connection Types](#connection-types)
- [Client-To-Server Messages](#client-to-server-messages)
  - [CONNECT](#connect)
  - [CONNECT_ACK](#connect_ack)
  - [IDENTIFY](#identify)
  - [IDENTIFY_ACK](#identify_ack)
  - [AUTH](#auth)
  - [AUTH_ACK](#auth_ack)
  - [JOIN](#join)
  - [JOIN_ACK](#join_ack)
  - [LEAVE](#leave)
  - [LEAVE_ACK](#leave_ack)
  - [BROADCAST](#broadcast)
  - [BROADCAST_ACK](#broadcast_ack)
  - [MESSAGE](#message)
  - [MOD_DIRECT](#mod_direct)
  - [MOD_DIRECT_ACK](#mod_direct_ack)
  - [CHANNELS](#channels)
  - [CHANNELS_ACK](#channels_ack)
  - [MEMBERS](#members)
  - [MEMBERS_ACK](#members_ack)
  - [GET_CHAN_ACL](#get_chan_acl)
  - [CHAN_ACL](#chan_acl)
  - [SET_CHAN_ACL](#set_chan_acl)
  - [GET_CHAN_CONFIG](#get_chan_config)
  - [CHAN_CONFIG](#chan_config)
  - [SET_CHAN_CONFIG](#set_chan_config)
  - [EVENT](#event)
  - [PING](#ping)
  - [PONG](#pong)
  - [ERROR](#error)
- [Server-To-Modulator Messages](#server-to-modulator-messages)
  - [S2M_CONNECT](#s2m_connect)
  - [S2M_CONNECT_ACK](#s2m_connect_ack)
  - [S2M_AUTH](#s2m_auth)
  - [S2M_AUTH_ACK](#s2m_auth_ack)
  - [S2M_FORWARD_EVENT](#s2m_forward_event)
  - [S2M_FORWARD_EVENT_ACK](#s2m_forward_event_ack)
  - [S2M_FORWARD_BROADCAST_PAYLOAD](#s2m_forward_broadcast_payload)
  - [S2M_FORWARD_BROADCAST_PAYLOAD_ACK](#s2m_forward_broadcast_payload_ack)
  - [S2M_MOD_DIRECT](#s2m_mod_direct)
  - [S2M_MOD_DIRECT_ACK](#s2m_mod_direct_ack)
- [Modulator-To-Server Messages](#modulator-to-server-messages)
  - [M2S_CONNECT](#m2s_connect)
  - [M2S_CONNECT_ACK](#m2s_connect_ack)
  - [M2S_MOD_DIRECT](#m2s_mod_direct)
  - [M2S_MOD_DIRECT_ACK](#m2s_mod_direct_ack)
- [Error Reasons](#error-reasons)
- [Event Kinds](#event-kinds)

## Overview

The Zyn protocol is designed for scalable pub/sub communication. It provides a low-level messaging infrastructure that can be extended with a custom application protocol through a **modulator**.

The protocol supports three types of connections:

1. **Client-to-Server (C2S)**: End-user clients connecting to the Zyn server
2. **Server-to-Modulator (S2M)**: Server-initiated connection to the modulator for delegating application-specific operations
3. **Modulator-to-Server (M2S)**: Modulator-initiated connection for sending direct messages to clients

### What is a Modulator?

A modulator is an external service that implements a custom application protocol on top of Zyn's messaging layer. Rather than embedding application logic in the server, Zyn delegates these concerns to a single modulator, keeping the core server lightweight and focused on message routing.

**Important**: Each Zyn server instance connects to exactly **one modulator**. This design ensures consistent application protocol semantics and simplifies the architecture.

**Common Use Cases:**

- **Custom Authentication**: Implement JWT validation, OAuth flows, or proprietary auth schemes
- **Authorization & Access Control**: Define complex permission rules beyond basic channel ACLs
- **Content Validation**: Enforce message schemas, size limits, or content policies
- **Message Transformation**: Encrypt, compress, or enrich messages before delivery
- **Business Logic**: Implement game logic, chat moderation, presence systems, or any application-specific features
- **Integration**: Bridge Zyn with external services, databases, or APIs
- **Analytics**: Track user behavior, message patterns, or system metrics

The modulator operates independently and can be written in any language, scaled separately, and updated without affecting the core Zyn server.

### Why a custom protocol?

Zyn implements a custom TCP-based protocol rather than leveraging existing protocols like HTTP or MQTT. This decision was made deliberately to optimize for the specific requirements of real-time pub/sub messaging while maintaining simplicity and flexibility.

**Key Design Rationale:**

1. **Fine-grained Control**: A custom protocol gives us complete control over every aspect of the wire format, allowing us to optimize for our exact use case. We can minimize overhead, eliminate unnecessary features, and ensure predictable behavior for real-time messaging patterns.

2. **Simplicity Through Constraint**: The protocol intentionally avoids complex features like nested types or recursive data structures. Message parameters are flat, consisting of simple scalar values (strings, integers) with optional binary payloads. This constraint makes the protocol easier to implement, debug, and reason about, while still providing all the functionality needed for pub/sub messaging.

3. **Efficiency Without Complexity**: By avoiding nested types and keeping message structures flat, we eliminate the need for complex parsing and serialization logic. This reduces CPU overhead, memory allocations, and latency—critical factors for real-time systems handling thousands of concurrent connections.

4. **Hybrid Design for Future Flexibility**: The protocol uses a hybrid approach with text-based message headers and binary payloads. This provides excellent debuggability during development (text headers are human-readable over the wire) while maintaining efficiency for payload delivery. Importantly, this design can be trivially migrated to a fully binary protocol if needed—the text headers can be replaced with binary-encoded message types and length-prefixed parameters without changing the protocol's semantics.

5. **Tailored Message Flow**: Generic protocols often impose constraints that don't align with our needs. For example, HTTP's request-response model doesn't naturally support bidirectional streaming or server-initiated messages. By building our own protocol, we can design message flows that perfectly match the pub/sub domain: connection handshakes, channel subscriptions, broadcasts, and modulator delegation.

6. **Predictable Performance**: With full control over the protocol, we can guarantee consistent performance characteristics. There are no hidden features, no protocol negotiation surprises, and no compatibility concerns with different protocol versions from various vendors. This predictability is crucial for production systems with strict latency requirements.

7. **Evolution Path**: The protocol's versioning mechanism (`CONNECT` message includes protocol version) gives us explicit control over evolution. We can introduce breaking changes through version negotiation rather than being constrained by backward compatibility requirements of established protocols.

## Preliminaries

### Network

Zyn uses a hybrid protocol over TCP with text-based message framing and binary payloads. The protocol defines all messages as request-response pairs or server-initiated notifications. Message headers are newline-delimited text consisting of a message name followed by space-separated parameters. Messages that include payloads (indicated by a `length` parameter) are followed by binary data.

The client initiates a socket connection and then writes a sequence of request messages and reads back the corresponding response messages. A handshake is required upon connection to establish protocol parameters and capabilities.

The server guarantees that on a single TCP connection, requests will be processed in the order they are sent, and responses will return in that order as well. However, the protocol supports multiple in-flight requests through a correlation ID mechanism, allowing clients to pipeline requests without waiting for each response.

The server has a configurable maximum limit on message size (`max_message_size`) and payload size (`max_payload_size`), communicated during connection establishment. Any message that exceeds these limits will result in an error or connection termination.

**Transport Layer Requirements:**

- **TCP**: All Zyn connections use TCP for reliable, ordered delivery
- **TLS**: Mandatory for client-to-server (C2S) connections to protect authentication tokens and message content. For modulator connections (M2S/S2M), plain TCP or Unix domain sockets are supported
- **Port**: Configurable, but clients must know the server's port to connect
- **Encoding**: Messages use UTF-8 text encoding for parameters, with binary payloads transmitted as opaque byte sequences

### Connection Management

Zyn supports three types of connections, each with its own lifecycle:

**Client-To-Server Connections (C2S)**:
- Initiated by clients using [CONNECT](#connect)
- May require authentication via [IDENTIFY](#identify) and [AUTH](#auth)
- Long-lived connections for ongoing message exchange
- Support heartbeat via [PING](#ping)/[PONG](#pong)
- Clients should reconnect with exponential backoff on disconnection

**Server-To-Modulator Connections (S2M)**:
- Initiated by the server using [S2M_CONNECT](#s2m_connect)
- Authenticate via shared secret
- Server delegates operations to the modulator
- Modulator declares supported operations in [S2M_CONNECT_ACK](#s2m_connect_ack)

**Modulator-To-Server Connections (M2S)**:
- Initiated by the modulator using [M2S_CONNECT](#m2s_connect)
- Authenticate via shared secret
- Only one modulator connection allowed per server
- Used by modulator to send direct messages to clients

**Connection Persistence:**

Zyn is designed for persistent connections. Clients and modulators should:
- Maintain long-lived TCP connections rather than connecting per-request
- Implement reconnection logic with exponential backoff for transient failures
- Send periodic heartbeats at the negotiated `heartbeat_interval` to detect dead connections
- Gracefully handle connection closures by attempting to reconnect

**Connection Limits:**

The server enforces the following limits, communicated during connection establishment:
- `max_inflight_requests`: Maximum number of concurrent outstanding requests per connection
- `max_subscriptions`: Maximum number of channels a client can join (client connections only)
- `max_message_size`: Maximum size of a protocol message in bytes
- `max_payload_size`: Maximum size of a binary payload in bytes

### Message Ordering and Flow Control

**Request-Response Ordering:**

The Zyn protocol guarantees that messages on a single TCP connection are processed in the order they are received. However, the protocol supports request pipelining through correlation IDs:

- Clients can send multiple requests without waiting for responses
- Each request that expects a response includes an `id` parameter (u16, 1-65535)
- Responses include the same `id` to correlate with the original request
- The server may process requests concurrently but returns responses in any order
- Clients must track outstanding requests by their correlation IDs

**Flow Control:**

The `max_inflight_requests` parameter limits the number of concurrent outstanding requests:
- Clients must not send more than `max_inflight_requests` requests before receiving responses
- This prevents overwhelming the server and provides backpressure
- Exceeding this limit may result in errors or connection termination

**Server-Initiated Messages:**

Some messages are sent by the server without a corresponding client request:
- [MESSAGE](#message): Broadcast messages from channels
- [MOD_DIRECT](#mod_direct): Direct messages (when initiated by modulator)
- [EVENT](#event): Channel events (member join/leave)
- [ERROR](#error): Errors without correlation IDs (connection-level errors)

These messages do not have correlation IDs, and are delivered asynchronously.

**Heartbeat:**

Both clients and modulators must implement heartbeat to detect dead connections:
- Send [PING](#ping) messages at the negotiated `heartbeat_interval`
- Server responds with [PONG](#pong) containing the same correlation ID
- Missing heartbeats (typically 2-3 intervals) indicate a dead connection
- Clients should close and reconnect when heartbeats fail

### Versioning and Compatibility

The Zyn protocol is designed to enable incremental evolution in a backward-compatible fashion. Versioning is done at the connection level, with each connection negotiating a protocol version during the initial handshake.

**Version Negotiation:**

All connections begin with a version negotiation:
- Client sends [CONNECT](#connect) with `version` parameter indicating the protocol version it supports
- Server responds with [CONNECT_ACK](#connect_ack) accepting the connection or [ERROR](#error) with `UNSUPPORTED_PROTOCOL_VERSION`
- If accepted, both parties communicate using the negotiated protocol version for the lifetime of the connection
- Similar negotiation occurs for modulator connections via [M2S_CONNECT](#m2s_connect)/[S2M_CONNECT](#s2m_connect)

**Version Semantics:**

- **Version 1**: Initial protocol specification (current version)
- Future versions will maintain backward compatibility where possible
- Breaking changes will require a new major version number
- The server may support multiple protocol versions simultaneously
- Clients must specify the exact version they implement

**Compatibility Strategy:**

The intended upgrade path is:
1. New protocol versions are first deployed on servers
2. Servers continue to support older protocol versions for backward compatibility
3. Clients are gradually updated to use newer protocol versions
4. Old protocol versions are deprecated after a transition period

**Modulator Operations:**

The modulator declares supported operations in its [S2M_CONNECT_ACK](#s2m_connect_ack) response:
- `operations`: Array of operation names the modulator supports (e.g., `auth`, `fwd-broadcast-payload`, etc.)
- The server adapts its behavior based on supported operations
- If `auth` is not supported, clients must use [IDENTIFY](#identify) instead of authenticated [AUTH](#auth)
- This allows modulators to implement only the features they need

**Error Handling:**

Version mismatches and unsupported features result in errors:
- `UNSUPPORTED_PROTOCOL_VERSION`: Client's protocol version is not supported by the server
- `NOT_IMPLEMENTED`: Requested feature is not implemented
- Clients should handle these errors gracefully and either retry with a different version or inform the user

## Protocol

### Identifiers

#### ZID (Zyn ID)

A ZID is a unique identifier for users and servers in the format `username@domain`.

- **Client ZID**: `username@domain` (e.g., `alice@example.com`)
- **Server ZID**: `domain` (e.g., `example.com`) - used when username is empty

**Validation Rules:**
- Username must be alphanumeric with hyphens, dots, or underscores
- Username maximum length: 256 characters
- Domain must be a valid domain name, IPv4, IPv6, or `localhost`
- Domain maximum length: 253 characters

#### ChannelId

A ChannelId identifies a communication channel in the format `!handler@domain`.

- Format: `!{handler}@{domain}` (e.g., `!123@example.com`)
- Handler is a non-zero unsigned 32-bit integer
- Domain follows the same validation rules as ZID domains

### Message Structure

Messages in the Zyn protocol follow a text-based format:

```
MESSAGE_NAME param1=value1 param2=value2 param3:2=value1,value2
```

- Message names are uppercase ASCII strings
- Parameters are space-separated key-value pairs
- Array parameters use the format `name:count=value1 value2 ...`
- Values with whitespace are escaped using delimiters (`\"`, `\'`, `\:`, or `\*`)

### Connection Types

#### Client Connection Flow

1. Client sends [CONNECT](#connect)
2. Server responds with [CONNECT_ACK](#connect_ack)
3. If auth required:
   - If modulator doesn't support `auth` operation:
     - Client sends [IDENTIFY](#identify) to register username
     - Server responds with [IDENTIFY_ACK](#identify_ack)
   - Client sends [AUTH](#auth)
   - Server responds with [AUTH_ACK](#auth_ack) (delegated to modulator if it supports `auth`)
4. Client can now send operational messages

#### Server-to-Modulator Connection Flow (S2M)

1. Server sends [S2M_CONNECT](#s2m_connect)
2. Modulator responds with [S2M_CONNECT_ACK](#s2m_connect_ack) with supported operations
3. Server can now delegate operations like authentication, event processing, and payload handling to the modulator

#### Modulator Connection Flow (M2S)

1. Modulator sends [M2S_CONNECT](#m2s_connect)
2. Server responds with [M2S_CONNECT_ACK](#m2s_connect_ack)
3. Modulator can now send direct messages and handle application logic

## Client-To-Server Messages

### CONNECT

Initiates a connection to the Zyn server.

**Direction**: Client → Server

**Parameters**:
- `version` (u16, required): Protocol version number (must be non-zero)
- `heartbeat_interval` (u32, optional): Requested heartbeat interval in milliseconds (0 = no heartbeat)

**Example**:
```
CONNECT version=1 heartbeat_interval=30000
```

---

### CONNECT_ACK

Acknowledges a connection request and provides server capabilities.

**Direction**: Server → Client

**Parameters**:
- `auth_required` (bool, required): Whether authentication is required
- `application_protocol` (string, optional): Application-specific protocol identifier
- `heartbeat_interval` (u32, required): Server-assigned heartbeat interval in milliseconds
- `max_subscriptions` (u32, required): Maximum number of channels a client can join
- `max_message_size` (u32, required): Maximum size of a message in bytes
- `max_payload_size` (u32, required): Maximum size of a payload in bytes
- `max_inflight_requests` (u32, required): Maximum number of concurrent requests

**Example**:
```
CONNECT_ACK auth_required=true heartbeat_interval=30000 max_subscriptions=100 max_message_size=4096 max_payload_size=1048576 max_inflight_requests=10
```

---

### IDENTIFY

Registers a username with the server.

**Direction**: Client → Server

**Parameters**:
- `username` (string, required): Desired username (must be non-empty)

**Example**:
```
IDENTIFY username=alice
```

**Note**: IDENTIFY is required when the configured modulator (if any) doesn't support the `auth` operation type. In such cases, the server cannot delegate authentication to the modulator, so the client must first register a username via IDENTIFY. If a modulator with `auth` support is configured, IDENTIFY can be skipped and authentication will be delegated to the modulator.

---

### IDENTIFY_ACK

Confirms username registration and provides the full ZID.

**Direction**: Server → Client

**Parameters**:
- `zid` (string, required): The assigned ZID (must be non-empty)

**Example**:
```
IDENTIFY_ACK zid=alice@example.com
```

---

### AUTH

Authenticates the client using a token. Authentication is only supported when a modulator is configured and supports the `auth` operation. In this case, the server delegates token validation to the modulator via [S2M_AUTH](#s2m_auth). If no modulator with `auth` support is configured, clients must use [IDENTIFY](#identify) instead to register a username, and the AUTH message serves no authentication purpose.

**Direction**: Client → Server

**Parameters**:
- `token` (string, required): Authentication token (must be non-empty)

**Example**:
```
AUTH token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Note**: Authentication is only possible when a modulator with `auth` support is configured. In this case, the modulator determines the username. Without a modulator supporting `auth`, the username must be set via [IDENTIFY](#identify) and no real authentication occurs.

---

### AUTH_ACK

Acknowledges an authentication attempt. If a modulator with `auth` support is configured, this response is based on the modulator's [S2M_AUTH_ACK](#s2m_auth_ack) response. Without such a modulator, this is simply an acknowledgment without actual authentication.

**Direction**: Server → Client

**Parameters**:
- `challenge` (string, optional): Challenge string for additional authentication steps
- `succeeded` (bool, optional): Whether authentication succeeded
- `zid` (string, optional): The authenticated user's ZID

**Example**:
```
AUTH_ACK succeeded=true zid=alice@example.com
```

**Note**: When a modulator with `auth` support is configured, the `zid` is constructed from the `username` provided by the modulator in [S2M_AUTH_ACK](#s2m_auth_ack) combined with the server's domain.

---

### JOIN

Requests to join a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, optional): Channel ID to join
- `on_behalf` (string, optional): ZID to join on behalf of (for privileged operations)

**Example**:
```
JOIN id=1 channel=!42@example.com
```

---

### JOIN_ACK

Acknowledges a channel join request.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the JOIN message (must be non-zero)
- `channel` (string, required): Channel ID that was joined

**Example**:
```
JOIN_ACK id=1 channel=!42@example.com
```

---

### LEAVE

Requests to leave a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to leave (must be non-empty)
- `on_behalf` (string, optional): ZID to remove from channel (for privileged operations)

**Example**:
```
LEAVE id=2 channel=!42@example.com
```

---

### LEAVE_ACK

Acknowledges a channel leave request.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the LEAVE message (must be non-zero)

**Example**:
```
LEAVE_ACK id=2
```

---

### BROADCAST

Sends a message to all members of a channel. This message includes a payload.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Target channel ID (must be non-empty)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example**:
```
BROADCAST id=3 channel=!42@example.com length=1024
[1024 bytes of binary payload follow]
```

---

### BROADCAST_ACK

Acknowledges a broadcast message.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the BROADCAST message (must be non-zero)

**Example**:
```
BROADCAST_ACK id=3
```

---

### MESSAGE

Receives a broadcast message from a channel. This message includes a payload.

**Direction**: Server → Client

**Parameters**:
- `from` (string, required): ZID of the sender (must be non-empty)
- `channel` (string, required): Channel ID where the message was sent (must be non-empty)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example**:
```
MESSAGE from=bob@example.com channel=!42@example.com length=512
[512 bytes of binary payload follow]
```

---

### MOD_DIRECT

Sends or receives a direct message to/from a modulator for application-specific communication. This message includes a payload.

**Direction**: Bidirectional (Client ↔ Server)

**Parameters**:
- `id` (u16, optional): Request identifier for client-initiated messages
- `from` (string, required): Sender ZID (must be non-empty)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example (Client → Server)**:
```
MOD_DIRECT id=4 from=alice@example.com length=256
[256 bytes of binary payload follow]
```

**Example (Server → Client)**:
```
MOD_DIRECT from=moderator@example.com length=128
[128 bytes of binary payload follow]
```

---

### MOD_DIRECT_ACK

Acknowledges a direct message to the modulator (when initiated by client).

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the MOD_DIRECT message (must be non-zero)

**Example**:
```
MOD_DIRECT_ACK id=4
```

---

### CHANNELS

Requests a list of channels.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `owner` (bool, required): If true, list only channels owned by the requester

**Example**:
```
CHANNELS id=5 owner=false
```

---

### CHANNELS_ACK

Returns a list of channels.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the CHANNELS message (must be non-zero)
- `channels` (string[], required): Array of channel IDs

**Example**:
```
CHANNELS_ACK id=5 channels:3=!42@example.com !43@example.com !44@example.com
```

---

### MEMBERS

Requests a list of members in a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to query (must be non-empty)

**Example**:
```
MEMBERS id=6 channel=!42@example.com
```

---

### MEMBERS_ACK

Returns a list of channel members.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the MEMBERS message (must be non-zero)
- `channel` (string, required): Channel ID (must be non-empty)
- `members` (string[], required): Array of member ZIDs

**Example**:
```
MEMBERS_ACK id=6 channel=!42@example.com members:2=alice@example.com bob@example.com
```

---

### GET_CHAN_ACL

Requests the access control list for a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to query (must be non-empty)

**Example**:
```
GET_CHAN_ACL id=7 channel=!42@example.com
```

---

### CHAN_ACL

Returns or sets the access control list for a channel.

**Direction**: Server → Client (in response to GET_CHAN_ACL)

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID (must be non-empty)
- `allow_join` (string[], required): Array of ZID patterns allowed to join
- `allow_publish` (string[], required): Array of ZID patterns allowed to publish
- `allow_read` (string[], required): Array of ZID patterns allowed to read

**Example**:
```
CHAN_ACL id=7 channel=!42@example.com allow_join:2=*@example.com *@trusted.com allow_publish:2=alice@example.com bob@example.com allow_read:1=*@example.com
```

---

### SET_CHAN_ACL

Sets the access control list for a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to modify (must be non-empty)
- `allow_join` (string[], required): Array of ZID patterns allowed to join
- `allow_publish` (string[], required): Array of ZID patterns allowed to publish
- `allow_read` (string[], required): Array of ZID patterns allowed to read

**Example**:
```
SET_CHAN_ACL id=8 channel=!42@example.com allow_join:2=*@example.com *@trusted.com allow_publish:2=alice@example.com bob@example.com allow_read:1=*@example.com
```

**Response**: [CHAN_ACL](#chan_acl) with the updated ACL or [ERROR](#error)

---

### GET_CHAN_CONFIG

Requests the configuration for a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to query (must be non-empty)

**Example**:
```
GET_CHAN_CONFIG id=9 channel=!42@example.com
```

---

### CHAN_CONFIG

Returns or sets the configuration for a channel.

**Direction**: Server → Client (in response to GET_CHAN_CONFIG)

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID (must be non-empty)
- `max_clients` (u32, required): Maximum number of clients allowed in the channel
- `max_payload_size` (u32, required): Maximum payload size in bytes

**Example**:
```
CHAN_CONFIG id=9 channel=!42@example.com max_clients=100 max_payload_size=1048576
```

---

### SET_CHAN_CONFIG

Sets the configuration for a channel.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, required): Channel ID to modify (must be non-empty)
- `max_clients` (u32, required): Maximum number of clients allowed in the channel
- `max_payload_size` (u32, required): Maximum payload size in bytes

**Example**:
```
SET_CHAN_CONFIG id=10 channel=!42@example.com max_clients=200 max_payload_size=2097152
```

**Response**: [CHAN_CONFIG](#chan_config) with the updated configuration or [ERROR](#error)

---

### EVENT

Notifies about a channel or system event.

**Direction**: Server → Client

**Parameters**:
- `kind` (string, required): Event type (must be non-empty, see [Event Kinds](#event-kinds))
- `channel` (string, optional): Channel ID where the event occurred
- `zid` (string, optional): ZID associated with the event
- `owner` (bool, optional): Whether the ZID is the channel owner

**Example**:
```
EVENT kind=MEMBER_JOINED channel=!42@example.com zid=bob@example.com owner=false
```

---

### PING

Heartbeat message to keep the connection alive.

**Direction**: Client → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)

**Example**:
```
PING id=100
```

---

### PONG

Response to a heartbeat message.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, required): Request identifier matching the PING message (must be non-zero)

**Example**:
```
PONG id=100
```

---

### ERROR

Indicates an error condition.

**Direction**: Server → Client

**Parameters**:
- `id` (u16, optional): Correlation identifier for the failed request
- `reason` (string, required): Error reason code (must be non-empty, see [Error Reasons](#error-reasons))
- `detail` (string, optional): Human-readable error details

**Example**:
```
ERROR id=1 reason=CHANNEL_NOT_FOUND detail=\:Channel !999@example.com does not exist\:
```

## Server-To-Modulator Messages

### S2M_CONNECT

Initiates a server connection to the modulator. Used when the server connects to its configured modulator to delegate application protocol operations.

**Direction**: Server → Modulator

**Parameters**:
- `version` (u16, required): Protocol version number (must be non-zero)
- `secret` (string, optional): Authentication secret
- `heartbeat_interval` (u32, required): Requested heartbeat interval in milliseconds

**Example**:
```
S2M_CONNECT version=1 secret=server-secret heartbeat_interval=30000
```

---

### S2M_CONNECT_ACK

Acknowledges a server-to-modulator connection and declares the modulator's capabilities.

**Direction**: Modulator → Server

**Parameters**:
- `application_protocol` (string, required): Modulator's application protocol (must be non-empty)
- `operations` (string[], required): Supported operations (must be non-empty)
- `heartbeat_interval` (u32, required): Heartbeat interval in milliseconds (must be non-zero)
- `max_inflight_requests` (u32, required): Maximum concurrent requests (must be non-zero)
- `max_message_size` (u32, required): Maximum message size in bytes (must be non-zero)
- `max_payload_size` (u32, required): Maximum payload size in bytes (must be non-zero)

**Example**:
```
Modulator → Server: S2M_CONNECT_ACK application_protocol=myapp-v1 operations:2=auth fwd-broadcast-payload heartbeat_interval=30000 max_inflight_requests=50 max_message_size=8192 max_payload_size=4194304
```

---

### S2M_AUTH

Delegates user authentication to the modulator's application protocol. This message is only sent if the modulator declared support for the `auth` operation in its [S2M_CONNECT_ACK](#s2m_connect_ack) message.

**Direction**: Server → Modulator

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `token` (string, required): Authentication token to validate (must be non-empty)

**Example**:
```
S2M_AUTH id=1 token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**Note**: If the modulator doesn't support `auth`, authentication is not available. Clients must use [IDENTIFY](#identify) to register a username, and the AUTH message does not provide real authentication.

---

### S2M_AUTH_ACK

Returns authentication result from the modulator's application protocol. This is sent in response to [S2M_AUTH](#s2m_auth).

**Direction**: Modulator → Server

**Parameters**:
- `id` (u16, required): Request identifier matching the S2M_AUTH message (must be non-zero)
- `challenge` (string, optional): Challenge for additional authentication steps
- `username` (string, optional): Authenticated username (the server combines this with its domain to form the full ZID)
- `succeeded` (bool, required): Whether authentication succeeded

**Example**:
```
S2M_AUTH_ACK id=1 username=alice succeeded=true
```

**Note**: The `username` provided here is used by the server to construct the client's ZID as `username@server-domain`.

---

### S2M_FORWARD_EVENT

Forwards a channel event to the modulator for application-specific processing.

**Direction**: Server → Modulator

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `channel` (string, optional): Channel ID where the event occurred
- `kind` (string, required): Event type (must be non-empty, see [Event Kinds](#event-kinds))
- `zid` (string, optional): ZID associated with the event
- `owner` (bool, optional): Whether the ZID is the channel owner

**Example**:
```
S2M_FORWARD_EVENT id=2 channel=!42@example.com kind=MEMBER_JOINED zid=bob@example.com owner=false
```

---

### S2M_FORWARD_EVENT_ACK

Acknowledges event processing by modulator.

**Direction**: Modulator → Server

**Parameters**:
- `id` (u16, required): Request identifier matching the S2M_FORWARD_EVENT message (must be non-zero)

**Example**:
```
S2M_FORWARD_EVENT_ACK id=2
```

---

### S2M_FORWARD_BROADCAST_PAYLOAD

Forwards a broadcast message payload to the modulator for application-specific validation, filtering, or modification. This message includes a payload.

**Direction**: Server → Modulator

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `from` (string, required): Sender ZID (must be non-empty)
- `channel` (u32, required): Channel handler number (must be non-zero)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example**:
```
S2M_FORWARD_BROADCAST_PAYLOAD id=3 from=alice@example.com channel=42 length=1024
[1024 bytes of binary payload follow]
```

---

### S2M_FORWARD_BROADCAST_PAYLOAD_ACK

Returns the processing result for a broadcast payload from the modulator. May include a modified payload.

**Direction**: Modulator → Server

**Parameters**:
- `id` (u16, required): Request identifier matching the S2M_FORWARD_BROADCAST_PAYLOAD message (must be non-zero)
- `valid` (bool, required): Whether the payload is allowed
- `altered_payload` (bool, required): Whether the payload was modified
- `altered_payload_length` (u32, required): Size of modified payload in bytes (0 if not altered)

**Example (payload rejected)**:
```
S2M_FORWARD_BROADCAST_PAYLOAD_ACK id=3 valid=false altered_payload=false altered_payload_length=0
```

**Example (payload accepted and modified)**:
```
S2M_FORWARD_BROADCAST_PAYLOAD_ACK id=3 valid=true altered_payload=true altered_payload_length=512
[512 bytes of modified payload follow]
```

---

### S2M_MOD_DIRECT

Forwards a direct message from a client to the modulator for application-specific processing. This message includes a payload.

**Direction**: Server → Modulator

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `from` (string, required): Sender ZID (must be non-empty)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example**:
```
S2M_MOD_DIRECT id=4 from=alice@example.com length=256
[256 bytes of binary payload follow]
```

---

### S2M_MOD_DIRECT_ACK

Acknowledges receipt and processing of a direct message by the modulator.

**Direction**: Modulator → Server

**Parameters**:
- `id` (u16, required): Request identifier matching the S2M_MOD_DIRECT message (must be non-zero)
- `valid` (bool, required): Whether the message was processed successfully

**Example**:
```
S2M_MOD_DIRECT_ACK id=4 valid=true
```

## Modulator-To-Server Messages

### M2S_CONNECT

Initiates the modulator connection to the server. Used when the modulator connects to the server to provide application protocol services.

**Direction**: Modulator → Server

**Parameters**:
- `version` (u16, required): Protocol version number (must be non-zero)
- `secret` (string, optional): Authentication secret to validate the modulator
- `heartbeat_interval` (u32, required): Requested heartbeat interval in milliseconds

**Example**:
```
M2S_CONNECT version=1 secret=modulator-secret-key heartbeat_interval=30000
```

**Note**: Only one modulator can be connected to a Zyn server at a time.

---

### M2S_CONNECT_ACK

Acknowledges a modulator connection.

**Direction**: Server → Modulator

**Parameters**:
- `heartbeat_interval` (u32, required): Server-assigned heartbeat interval (must be non-zero)
- `max_inflight_requests` (u32, required): Maximum concurrent requests (must be non-zero)
- `max_message_size` (u32, required): Maximum message size in bytes (must be non-zero)
- `max_payload_size` (u32, required): Maximum payload size in bytes (must be non-zero)

**Example**:
```
M2S_CONNECT_ACK heartbeat_interval=30000 max_inflight_requests=100 max_message_size=4096 max_payload_size=2097152
```

---

### M2S_MOD_DIRECT

Sends a direct message from modulator to specific clients for application-specific communication. This message includes a payload.

**Direction**: Modulator → Server

**Parameters**:
- `id` (u16, required): Request identifier (must be non-zero)
- `targets` (string[], required): Array of target ZIDs (must be non-empty)
- `length` (u32, required): Payload size in bytes (must be non-zero)

**Example**:
```
M2S_MOD_DIRECT id=1 targets:2=alice@example.com bob@example.com length=512
[512 bytes of binary payload follow]
```

---

### M2S_MOD_DIRECT_ACK

Acknowledges a direct message from modulator.

**Direction**: Server → Modulator

**Parameters**:
- `id` (u16, required): Request identifier matching the M2S_MOD_DIRECT message (must be non-zero)

**Example**:
```
M2S_MOD_DIRECT_ACK id=1
```

## Error Reasons

The following error reasons can be returned in [ERROR](#error) messages:

| Error Code | Description |
|------------|-------------|
| `BAD_REQUEST` | The request is malformed or contains invalid parameters |
| `CHANNEL_NOT_FOUND` | The specified channel does not exist |
| `CHANNEL_IS_FULL` | The channel has reached its maximum capacity |
| `FORBIDDEN` | The request is not allowed due to lack of permissions |
| `INTERNAL_SERVER_ERROR` | An unexpected error occurred on the server |
| `MESSAGE_CHANNEL_FULL` | The message channel buffer is full |
| `NOT_ALLOWED` | The operation is not allowed for the current user or context |
| `NOT_IMPLEMENTED` | The requested feature or operation is not yet implemented |
| `POLICY_VIOLATION` | The request violates server policies or rules |
| `SERVER_OVERLOADED` | The server is overloaded and cannot handle the request |
| `SERVER_SHUTTING_DOWN` | The server is shutting down and not accepting new requests |
| `TIMEOUT` | The operation timed out before completion |
| `UNAUTHORIZED` | The user/server is not authorized to perform the operation |
| `UNEXPECTED_MESSAGE` | Received a message not expected in the current protocol state |
| `UNSUPPORTED_PROTOCOL_VERSION` | The client's protocol version is not supported |
| `USER_IN_CHANNEL` | The user is already a member of the specified channel |
| `USER_NOT_IN_CHANNEL` | The user is not a member of the specified channel |
| `USERNAME_IN_USE` | The requested username is already taken |
| `USER_NOT_REGISTERED` | The user has not completed the registration process |

### Recoverable vs Non-Recoverable Errors

**Recoverable errors** (client can retry or take corrective action):
- `CHANNEL_IS_FULL`
- `CHANNEL_NOT_FOUND`
- `FORBIDDEN`
- `NOT_ALLOWED`
- `NOT_IMPLEMENTED`
- `USER_IN_CHANNEL`
- `USER_NOT_IN_CHANNEL`
- `USERNAME_IN_USE`
- `USER_NOT_REGISTERED`
- `SERVER_OVERLOADED`

**Non-recoverable errors** (connection should be closed):
- `BAD_REQUEST`
- `INTERNAL_SERVER_ERROR`
- `POLICY_VIOLATION`
- `SERVER_SHUTTING_DOWN`
- `MESSAGE_CHANNEL_FULL`
- `TIMEOUT`
- `UNAUTHORIZED`
- `UNEXPECTED_MESSAGE`
- `UNSUPPORTED_PROTOCOL_VERSION`

## Event Kinds

The following event kinds can be sent in [EVENT](#event) and [S2M_FORWARD_EVENT](#s2m_forward_event) messages:

| Event Kind | Description |
|------------|-------------|
| `MEMBER_JOINED` | A member has joined a channel |
| `MEMBER_LEFT` | A member has left a channel |

### Event Context

Events contain contextual information that varies depending on the event kind:

- **MEMBER_JOINED**: Includes `channel`, `zid` (the member who joined), and `owner` (whether they are the channel owner)
- **MEMBER_LEFT**: Includes `channel`, `zid` (the member who left), and `owner` (whether they were the channel owner)

## Protocol Flow Examples

### Example 1: Basic Client Connection and Channel Join

```
Client → Server: CONNECT version=1 heartbeat_interval=30000
Server → Client: CONNECT_ACK auth_required=true heartbeat_interval=30000 max_subscriptions=100 max_message_size=4096 max_payload_size=1048576 max_inflight_requests=10

Client → Server: IDENTIFY username=alice
Server → Client: IDENTIFY_ACK zid=alice@example.com

Client → Server: AUTH token=abc123token
Server → Client: AUTH_ACK succeeded=true zid=alice@example.com

Client → Server: JOIN id=1 channel=!42@example.com
Server → Client: JOIN_ACK id=1 channel=!42@example.com
Server → Client: EVENT kind=MEMBER_JOINED channel=!42@example.com zid=alice@example.com owner=false
```

### Example 2: Broadcasting a Message

```
Client → Server: BROADCAST id=5 channel=!42@example.com length=13
Client → Server: [payload: "Hello, World!"]
Server → Client: BROADCAST_ACK id=5

[Server forwards to all channel members]
Server → Other Clients: MESSAGE from=alice@example.com channel=!42@example.com length=13
Server → Other Clients: [payload: "Hello, World!"]
```

### Example 3a: Authentication with Modulator Support

When a modulator supports the `auth` operation, it handles authentication and provides the username:

```
Server → Modulator: S2M_CONNECT version=1 secret=server-secret heartbeat_interval=30000
Modulator → Server: S2M_CONNECT_ACK application_protocol=myapp-v1 operations:2=auth fwd-broadcast-payload heartbeat_interval=30000 max_inflight_requests=50 max_message_size=8192 max_payload_size=4194304

[Client connects and authenticates directly without IDENTIFY]
Client → Server: CONNECT version=1 heartbeat_interval=30000
Server → Client: CONNECT_ACK auth_required=true heartbeat_interval=30000 max_subscriptions=100 max_message_size=4096 max_payload_size=1048576 max_inflight_requests=10

Client → Server: AUTH token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Server → Modulator: S2M_AUTH id=1 token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
Modulator → Server: S2M_AUTH_ACK id=1 username=alice succeeded=true
Server → Client: AUTH_ACK succeeded=true zid=alice@example.com

[Client can now send direct messages to the modulator]
Client → Server: MOD_DIRECT id=1 from=alice@example.com length=20
Client → Server: [payload: "APP_SPECIFIC_REQUEST"]
Server → Modulator: S2M_MOD_DIRECT id=2 from=alice@example.com length=20
Server → Modulator: [payload: "APP_SPECIFIC_REQUEST"]
Modulator → Server: S2M_MOD_DIRECT_ACK id=2 valid=true
```

### Example 3b: Authentication without Modulator Auth Support

When a modulator doesn't support the `auth` operation, clients must use IDENTIFY first:

```
Server → Modulator: S2M_CONNECT version=1 secret=server-secret heartbeat_interval=30000
Modulator → Server: S2M_CONNECT_ACK application_protocol=myapp-v1 operations:1=fwd-broadcast-payload heartbeat_interval=30000 max_inflight_requests=50 max_message_size=8192 max_payload_size=4194304

[Client connects and must identify before authenticating]
Client → Server: CONNECT version=1 heartbeat_interval=30000
Server → Client: CONNECT_ACK auth_required=true heartbeat_interval=30000 max_subscriptions=100 max_message_size=4096 max_payload_size=1048576 max_inflight_requests=10

Client → Server: IDENTIFY username=alice
Server → Client: IDENTIFY_ACK zid=alice@example.com

Client → Server: AUTH token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
[Server does not validate token - no authentication occurs without modulator auth support]
Server → Client: AUTH_ACK succeeded=true zid=alice@example.com
```

### Example 4: Error Handling

```
Client → Server: JOIN id=2 channel=!999@example.com
Server → Client: ERROR id=2 reason=CHANNEL_NOT_FOUND detail=\:Channel !999@example.com does not exist\:
```

## Wire Format Details

### Parameter Encoding

Parameters are encoded as space-separated key-value pairs:

- **Simple values**: `key=value`
- **String values with whitespace**: Escaped using one of `\"`, `\'`, `\:`, or `\*`
  - Example: `detail=\:This is a message with spaces\:`
- **Empty strings**: `key=\"\"`
- **Boolean values**: `key=true` or `key=false`
- **Numeric values**: `key=123`
- **Array values**: `key:count=value1 value2 ...`
  - Example: `members:3=alice@example.com bob@example.com charlie@example.com`

### Message Format

```
MESSAGE_NAME[ param1=value1][ param2=value2]...\n
[optional binary payload]
```

- Message name is uppercase ASCII (text)
- Parameters are optional and space-separated (text)
- Message header ends with a newline character (`\n`)
- If the message includes a payload (indicated by a `length` parameter), the binary payload immediately follows the newline
- Payloads are opaque binary data, not text-encoded

### Payload Messages

The following messages include binary payloads:

- [BROADCAST](#broadcast) / [MESSAGE](#message)
- [MOD_DIRECT](#mod_direct) / [M2S_MOD_DIRECT](#m2s_mod_direct) / [S2M_MOD_DIRECT](#s2m_mod_direct)
- [S2M_FORWARD_BROADCAST_PAYLOAD](#s2m_forward_broadcast_payload) / [S2M_FORWARD_BROADCAST_PAYLOAD_ACK](#s2m_forward_broadcast_payload_ack) (when `altered_payload=true`)

The payload immediately follows the message parameters without any additional framing.

## Security Considerations

### Authentication

- **Client Authentication**: Only supported when a modulator is configured with `auth` operation support
  - Clients with modulator auth: Use [AUTH](#auth) message with a token, validated by modulator via [S2M_AUTH](#s2m_auth)
  - Clients without modulator auth: Use [IDENTIFY](#identify) to register a username (no actual authentication occurs)
- **Modulator Authentication**: Modulators authenticate to the server using a shared secret in [M2S_CONNECT](#m2s_connect) or [S2M_CONNECT](#s2m_connect)
- **Transport Security**:
  - Client connections (C2S) **must** use TLS to protect authentication tokens and message content
  - Modulator connections (M2S/S2M) can use plain TCP or Unix domain sockets since they typically run in trusted environments

### Authorization

- Channel access is controlled through ACLs (Access Control Lists)
- ACLs specify which ZID patterns can join, publish, and read from channels
- The `on_behalf` parameter in [JOIN](#join) and [LEAVE](#leave) allows privileged users to manage other users' channel memberships

### Rate Limiting

- Servers enforce limits through configuration parameters:
  - `max_inflight_requests`: Limits concurrent outstanding requests
  - `max_message_size`: Limits individual message size
  - `max_payload_size`: Limits payload size
  - `max_subscriptions`: Limits number of channels per client

### Application Protocol Integration

- A single modulator provides the custom application protocol layer on top of Zyn's messaging infrastructure
- The [S2M_FORWARD_BROADCAST_PAYLOAD](#s2m_forward_broadcast_payload) flow allows payload validation, transformation, or enrichment
- [MOD_DIRECT](#mod_direct) / [M2S_MOD_DIRECT](#m2s_mod_direct) / [S2M_MOD_DIRECT](#s2m_mod_direct) enable bidirectional application-specific communication
- The modulator can implement custom authorization, content policies, message routing, or any other application logic
- Each Zyn server connects to exactly one modulator, ensuring consistent application semantics across all clients

## Implementation Notes

### Message Correlation

Messages that expect responses use an `id` parameter for correlation:
- The `id` must be unique among inflight requests from the same connection
- Responses include the same `id` to match requests
- `id` is a 16-bit unsigned integer (1-65535, 0 is reserved)

### Heartbeat

- Clients and modulators should send [PING](#ping) messages at the negotiated `heartbeat_interval`
- Servers respond with [PONG](#pong) messages
- Missing heartbeats may result in connection termination

### Connection Lifecycle

1. **Handshake**: Establish connection parameters and capabilities
2. **Authentication**: Verify identity (if required)
3. **Operations**: Perform channel operations and message exchange
4. **Teardown**: Graceful disconnection or error-based termination

### Error Recovery

- Recoverable errors allow clients to retry or adjust their requests
- Non-recoverable errors typically require reconnection
- Clients should implement exponential backoff for reconnection attempts

## Version History

- **Version 1**: Initial protocol specification

## License

This protocol is part of the Zyn project and is licensed under AGPL-3.0.

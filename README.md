# ⚡ Zyn

[![CI](https://img.shields.io/github/actions/workflow/status/zyn-org/zyn/ci.yaml?branch=main)](https://github.com/zyn-org/zyn/actions)
[![Releases](https://img.shields.io/github/v/release/zyn-org/zyn?include_prereleases)](https://github.com/zyn-org/zyn/releases)
[![LICENSE](https://img.shields.io/github/license/zyn-org/zyn)](https://github.com/zyn-org/zyn/blob/master/LICENSE)

A high-performance real-time messaging server for pub/sub communication.

## ✨ Features

- **Real-time Pub/Sub Messaging**: Low-latency message delivery across channels with broadcast support
- **Modular Architecture**: Extend the server with custom application logic via an external modulator
- **Secure by Default**: TLS/SSL support with automatic certificate generation for development
- **Flexible Authentication**: Delegate auth to a modulator for custom JWT, OAuth, or proprietary schemes
- **Channel Management**: Fine-grained access control and configuration per channel
- **High Performance**: Asynchronous Rust implementation with efficient message routing

## 🎬 Demo

https://github.com/user-attachments/assets/34baf7d3-4cfa-440d-a6e4-89cb94e922d3

## 🚀 Quick Start

### Prerequisites

- Rust 1.75 or later
- OpenSSL

### Installation

#### Building from Source

```bash
git clone https://github.com/zyn-org/zyn.git
cd zyn
cargo build --release
```

The compiled binary will be available at `target/release/zyn-server`.

#### Running the Server

```bash
# Run with default configuration
cargo run --bin zyn-server

# Or with a custom config file
cargo run --bin zyn-server -- --config path/to/config.toml
```

### Testing the Connection

Once the server is running, you can test the connection using OpenSSL:

```bash
openssl s_client -connect 127.0.0.1:22622 -ign_eof
```

## 📖 What is Zyn?

Zyn is a real-time messaging server that implements a protocol designed for scalable pub/sub communication. Unlike traditional message brokers, Zyn provides a low-level infrastructure that delegates custom application logic to an external **modulator**.

### What is a Modulator?

A modulator is an external service that implements custom application logic on top of Zyn's messaging layer. Rather than embedding application-specific features in the server, Zyn delegates these concerns to a modulator, keeping the core server lightweight and focused on message routing.

Each Zyn server connects to exactly **one modulator**, ensuring consistent application protocol semantics.

**Common Modulator Use Cases:**

- **Custom Authentication**: JWT validation, OAuth flows, or proprietary auth schemes
- **Authorization & Access Control**: Complex permission rules beyond basic channel ACLs
- **Content Validation**: Message schemas, size limits, or content policies
- **Message Transformation**: Encryption, compression, or message enrichment
- **Business Logic**: Game logic, chat moderation, presence systems
- **Integration**: Bridge with external services, databases, or APIs
- **Analytics**: Track user behavior and message patterns

## 🏗️ Architecture

Zyn supports three connection types:

1. **Client-to-Server (C2S)**: End-user clients connecting to the Zyn server
2. **Server-to-Modulator (S2M)**: Server-initiated connection to the modulator for delegating operations
3. **Modulator-to-Server (M2S)**: Modulator-initiated connection for sending private messages to clients

```
┌─────────┐         ┌──────────────┐          ┌───────────┐
│ Clients │ ◄─────► │  Zyn Server  │ ◄─────►  │ Modulator │
└─────────┘   C2S   └──────────────┘  S2M/M2S └───────────┘
```

## 🔧 Configuration

Zyn uses TOML format for configuration. See the [`examples/config/`](examples/config/) directory for examples.

## 📚 Documentation

- **[Protocol Specification](docs/PROTOCOL.md)**: Complete protocol documentation including message formats, flow examples, and wire format details
- **[Code of Conduct](CODE_OF_CONDUCT.md)**: Community guidelines
- **[Contributing Guide](CONTRIBUTING.md)**: How to contribute to the project

## 💡 Examples

The repository includes several example modulators in the [`examples/modulator/`](examples/modulator/) directory:

- **plain-authenticator**: Simple username/password authentication
- **broadcast-payload-json-validator**: Validates JSON message payloads
- **broadcast-payload-csv-validator**: Validates CSV message payloads
- **private-payload-sender**: Demonstrates sending private messages to clients

Each example demonstrates different aspects of building modulators for Zyn.

## 🛠️ Development

### Project Structure

```
zyn/
├── crates/
│   ├── common/          # Shared types and utilities
│   ├── modulator/       # Modulator client/server implementation
│   ├── protocol/        # Protocol message definitions
│   ├── protocol-macros/ # Protocol code generation macros
│   ├── server/          # Main Zyn server
│   ├── test-util/       # Testing utilities
│   └── util/            # General utilities
├── docs/                # Documentation
├── examples/            # Example configurations and modulators
└── README.md
```

### Running Tests

```bash
cargo test
```

### Running with Debug Tracing

```bash
RUST_LOG=debug cargo run --bin zyn-server
```

## ⚠️ Project Status

**Current Version: 0.1.0 (Alpha)**

Zyn is in active development and currently in **alpha** stage. While the core functionality is working and tested, please note:

- 🔧 **APIs may change** before reaching 1.0.0 - Breaking changes may occur as we refine the protocol and interfaces based on community feedback
- 🧪 **Evaluation and development use** - Suitable for testing, proof-of-concepts, and non-production environments
- 📣 **Community feedback welcome** - We're actively seeking input to improve Zyn before stabilizing the 1.0.0 API

If you're interested in using Zyn in production, we encourage you to get involved, provide feedback, and help shape the future of the project!

## 🗺️ Roadmap

We're actively working on expanding Zyn's capabilities. Here are some features planned for future releases:

- **🌐 Federation Support**: Enable multiple Zyn servers to communicate and share messages across distributed deployments, allowing for horizontal scaling and multi-region architectures
- **📊 Enhanced Observability**: Built-in metrics, tracing, and monitoring capabilities
- **🔌 Additional Protocol Transports**: Support for WebSocket and other transport layers
- **⚡ Performance Optimizations**: Continued improvements to throughput and latency

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details on:

- Reporting bugs
- Suggesting features
- Submitting pull requests
- Development setup

## 📜 License

This project is licensed under the AGPL-3.0 License - see the [LICENSE](LICENSE) file for details.

## 📞 Community

- **Issues**: [GitHub Issues](https://github.com/zyn-org/zyn/issues)
- **Discussions**: [GitHub Discussions](https://github.com/zyn-org/zyn/discussions)

---

Built with ⚡ by the Zyn team

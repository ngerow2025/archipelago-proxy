# archipelago-proxy

`archipelago-proxy` accepts WebSocket connections locally and forwards them to
an Archipelago WebSocket server. It supports plaintext WebSockets (`ws`) and
TLS WebSockets (`wss`) on both sides of the proxy.

## Requirements

- Rust and Cargo
- Network access to the target WebSocket server

## Build

Build a release binary with:

```sh
cargo build --release
```

The binary is written to `target/release/archipelago-proxy`.

## Usage

Pass the target WebSocket address as the first argument. The optional second
argument sets the local listening port and defaults to `38281`.

```text
archipelago-proxy <TARGET> [INPUT]
```

Examples:

```sh
# Forward local port 38281 to a plaintext WebSocket server. Port 38281 is the default is none is spesified.
archipelago-proxy ws://archipelago.example:38281

# Forward local port 40000 to a TLS WebSocket server.
archipelago-proxy wss://archipelago.example:443 40000
```

Clients should connect to `localhost:<INPUT>`.
Run the help command to see the command-line metadata and version:

```sh
archipelago-proxy --help
archipelago-proxy --version
```

## Certificates

When the proxy starts, it creates a self-signed certificate and private key in
`certs/cert.pem` and `certs/key.pem` if they do not already exist. These files
are used for incoming TLS connections. A TLS client must trust this
self-signed certificate, or use plaintext locally instead.

For Chrome:
- Go to chrome://certificate-manager/localcerts/usercerts and select "import" and navigate to the `certs/cert.pem` file.

For Firefox:
- You dont need to do this, firefox will trust localhost and allow a unsecure connection to the proxy.

## Logging

Set `RUST_LOG` to control log output, this is only needed for debugging purposes. For example:

```sh
RUST_LOG=info cargo run -- ws://archipelago.example:38281
RUST_LOG=debug cargo run -- ws://archipelago.example:38281
```

## Prebuilt releases

Tagged releases publish binaries for:

- Linux: `x86_64-unknown-linux-gnu`
- macOS: `x86_64-apple-darwin`
- Windows: `x86_64-pc-windows-msvc`

Download the archive for your platform from the repository's GitHub Releases
page, then run the extracted `archipelago-proxy` binary with the same arguments
shown above.
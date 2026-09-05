use std::error::Error;
use std::fs::File;
use std::io::{self, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, future};
use log::{debug, info, warn};
use rcgen::CertifiedKey;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certfile = File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(certfile)).collect()
}

fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    rustls_pemfile::private_key(&mut BufReader::new(File::open(path)?))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "private key not found"))
}

fn create_tls_acceptor() -> Result<TlsAcceptor, Box<dyn Error>> {
    let cert_path = Path::new("certs/cert.pem");
    let key_path = Path::new("certs/key.pem");

    if !cert_path.exists() || !key_path.exists() {
        info!("generating self-signed certificate and key");
        let CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
        std::fs::create_dir_all("certs")?;
        std::fs::write(cert_path, cert.pem())?;
        std::fs::write(key_path, signing_key.serialize_pem())?;
    }

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(load_certs(cert_path)?, load_key(key_path)?)?;

    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let args = Cli::parse();
    let tls_acceptor = create_tls_acceptor()?;

    println!(
        "starting proxy on port {} to {}://{}:{}",
        args.input, args.output.scheme, args.output.url, args.output.port
    );

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.input)).await?;

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        debug!("accepted TCP connection from {}", peer_addr);
        let tls_acceptor = tls_acceptor.clone();

        tokio::spawn(async move {
            handle_conn(stream, peer_addr, tls_acceptor, args.output).await;
        });
    }
}
async fn handle_conn(stream: TcpStream, peer: SocketAddr, tls_acceptor: TlsAcceptor, target: URLPort) {
    info!("{}: new connection accepted", peer);

    let mut first_byte = [0u8; 1];
    let bytes_read = match stream.peek(&mut first_byte).await {
        Ok(bytes_read) => bytes_read,
        Err(error) => {
            warn!("{}: failed to inspect incoming connection: {}", peer, error);
            return;
        }
    };

    if bytes_read == 0 {
        warn!("{}: connection closed before sending data", peer);
    } else if first_byte[0] == 0x16 {
        info!("{}: detected incoming TLS connection", peer);
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => handle_ws(tls_stream, peer, target).await,
            Err(error) => warn!("{}: incoming TLS handshake failed: {}", peer, error),
        }
    } else {
        info!("{}: detected incoming plaintext connection", peer);
        handle_ws(stream, peer, target).await;
    }

    info!("{}: connection handler finished", peer);
}

async fn handle_ws<S>(stream: S, peer: SocketAddr, target: URLPort)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    debug!("{}: awaiting WebSocket handshake from client", peer);
    let incoming_connection = match tokio_tungstenite::accept_async(stream).await {
        Ok(ws) => {
            info!("{}: client WebSocket handshake completed", peer);
            ws
        }
        Err(e) => {
            warn!("{}: failed to accept incoming connection: {}", peer, e);
            return;
        }
    };

    debug!(
        "{}: connecting to target {}://{}:{}",
        peer, target.scheme, target.url, target.port
    );
    let target_connection = match tokio_tungstenite::connect_async_tls_with_config(
        format!("{}://{}:{}", target.scheme, target.url, target.port),
        None,
        false,
        None,
    )
    .await
    {
        Ok((ws, response)) => {
            info!(
                "{}: connected to target {}://{}:{} (status: {})",
                peer,
                target.scheme,
                target.url,
                target.port,
                response.status()
            );
            ws
        }
        Err(e) => {
            warn!(
                "{}: failed to connect to target {}://{}:{}: {}",
                peer, target.scheme, target.url, target.port, e
            );
            return;
        }
    };

    info!(
        "{}: forwarding messages between client and {}://{}:{}",
        peer, target.scheme, target.url, target.port
    );

    let (client_writer, client_reader) = incoming_connection.split();
    let (target_writer, target_reader) = target_connection.split();

    let mut client_to_target_count: u64 = 0;
    let mut target_to_client_count: u64 = 0;

    let client_to_target = client_reader
        .inspect_ok(|msg| {
            client_to_target_count += 1;
            if client_to_target_count % 100 == 0 {
                debug!(
                    "{}: forwarded {} client messages to target",
                    peer, client_to_target_count
                );
            }
            debug!("{}: client packet: {:?}", peer, msg);
        })
        .try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(target_writer);

    let target_to_client = target_reader
        .inspect_ok(|msg| {
            target_to_client_count += 1;
            if target_to_client_count % 100 == 0 {
                debug!(
                    "{}: forwarded {} target messages to client",
                    peer, target_to_client_count
                );
            }
            debug!("{}: target packet: {:?}", peer, msg);
        })
        .try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(client_writer);

    let result = tokio::select! {
        result = client_to_target => result.map(|()| "client-to-target"),
        result = target_to_client => result.map(|()| "target-to-client"),
    };

    match result {
        Ok(_direction) => info!(
            "{}: connection closed normally; client->target: {}, target->client: {}",
            peer, client_to_target_count, target_to_client_count
        ),
        Err(e) => warn!(
            "{}: error while forwarding; client->target: {}, target->client: {}: {}",
            peer, client_to_target_count, target_to_client_count, e
        ),
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about = "a WebSocket proxy supporting plaintext, TLS, and self-signed certificates", long_about = None)]
struct Cli {
    output: URLPort,
    #[arg(default_value = "38281")]
    input: u32,
}

#[derive(Debug, Clone, Copy)]
struct URLPort {
    scheme: &'static str,
    url: &'static str,
    port: u32,
}

impl FromStr for URLPort {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (scheme, address) = s.split_once("://").unwrap_or(("ws", s));
        if scheme != "ws" && scheme != "wss" {
            return Err("Invalid scheme. Expected ws or wss".to_string());
        }

        let (url, port) = address
            .rsplit_once(':')
            .ok_or_else(|| "Invalid format. Expected [ws://|wss://]<url>:<port>".to_string())?;
        let port = port
            .parse::<u32>()
            .map_err(|_| "Invalid port number".to_string())?;
        if url.is_empty() || port == 0 || port > u16::MAX as u32 {
            return Err("Invalid host or port".to_string());
        }

        Ok(URLPort {
            scheme: scheme.to_string().leak(),
            url: url.to_string().leak(),
            port,
        })
    }
}

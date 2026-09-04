use std::fs::File;
use std::io::{self, BufReader};
use std::sync::Arc;
use std::{path::Path, str::FromStr};
use std::error::Error;
use std::net::SocketAddr;

use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, future};
use log::{info, warn, debug};
use rcgen::CertifiedKey;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;

const ENABLE_INCOMING_TLS: bool = false;



// Helper function to load certificates from a PEM file
fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let certfile = File::open(path)?;
    let mut reader = BufReader::new(certfile);
    rustls_pemfile::certs(&mut reader).collect()
}

// Helper function to load the private key from a PEM file
fn load_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let keyfile = File::open(path)?;
    let mut reader = BufReader::new(keyfile);
    rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Private key not found"))
}



#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box< dyn Error>> {
    env_logger::init();

    let args = Cli::parse();


    // check if cert and key files exist, if not, generate them
    if !Path::new("certs/cert.pem").exists() || !Path::new("certs/key.pem").exists() {
        info!("Generating self-signed certificate and key...");
        let CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = cert.pem();
        let key_pem = signing_key.serialize_pem();

        std::fs::create_dir_all("certs").unwrap();
        std::fs::write("certs/cert.pem", cert_pem).unwrap();
        std::fs::write("certs/key.pem", key_pem).unwrap();
        //also write pkcs12 file
        let pkcs12 = signing_key.p
    }


    println!("starting proxy on port {} to {}:{}", args.input, args.output.url, args.output.port);



    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(load_certs(Path::new("certs/cert.pem")).unwrap(), load_key(Path::new("certs/key.pem")).unwrap())
        .unwrap();

    server_config.alpn_protocols = vec!["http/1.1".into(), "h2".into()];

    let acceptor = TlsAcceptor::from(Arc::new(server_config));


    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.input)).await?;



    loop {
        let (stream, peer_addr) = listener.accept().await?;
        debug!("accepted raw TCP connection from {}", peer_addr);
        let tls_acceptor = acceptor.clone();

        tokio::spawn(async move {
            handle_conn(stream, peer_addr, tls_acceptor, args.output).await;
        });

    }
}
async fn handle_conn(stream: TcpStream, peer: SocketAddr, tls_acceptor: TlsAcceptor, target: URLPort) {
    info!("{}: new connection accepted", peer);

    let mut peek_buf = [0u8; 1];
    let n = match stream.peek(&mut peek_buf).await {
        Ok(n) => n,
        Err(e) => {
            warn!("{}: peek failed: {}", peer, e);
            return;
        }
    };
    debug!("{}: peeked {} byte(s), first byte = {:#04x}", peer, n, peek_buf.get(0).copied().unwrap_or(0));

    const TLS_HANDSHAKE_BYTE: u8 = 0x16;

    if n > 0 && peek_buf[0] == TLS_HANDSHAKE_BYTE && ENABLE_INCOMING_TLS {
        info!("{}: detected TLS ClientHello -> wss", peer);
        match tls_acceptor.accept(stream).await {
            Ok(tls_stream) => {
                info!("{}: TLS handshake succeeded", peer);
                handle_ws(tls_stream, peer, target).await
            }
            Err(e) => warn!("{}: TLS handshake failed: {}", peer, e),
        }
    } else if n > 0 && peek_buf[0] == TLS_HANDSHAKE_BYTE {
        warn!("{}: incoming TLS connections are disabled, dropping connection", peer);
    } else if n == 0 {
        warn!("{}: connection closed before any data was sent", peer);
    } else {
        info!("{}: detected plaintext -> ws", peer);
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

    debug!("{}: connecting to target ws://{}:{}", peer, target.url, target.port);
    let target_connection = match tokio_tungstenite::connect_async(
        format!("ws://{}:{}", target.url, target.port)
    ).await {
        Ok((ws, response)) => {
            info!(
                "{}: connected to target {}:{} (status: {})",
                peer, target.url, target.port, response.status()
            );
            ws
        }
        Err(e) => {
            warn!("{}: failed to connect to target {}:{}: {}", peer, target.url, target.port, e);
            return;
        }
    };

    info!("{}: forwarding messages between client and {}:{}", peer, target.url, target.port);

    let mut forwarded: u64 = 0;
    let result = incoming_connection
        .inspect_ok(|msg| {
            forwarded += 1;
            if forwarded % 100 == 0 {
                debug!("{}: forwarded {} messages so far", peer, forwarded);
            }
        })
        .try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(target_connection)
        .await;

    match result {
        Ok(()) => info!("{}: connection closed normally after {} messages", peer, forwarded),
        Err(e) => warn!("{}: error while forwarding messages after {} messages: {}", peer, forwarded, e),
    }
}


#[derive(Parser, Debug)]
#[command(author, version, about = "a simple proxy for websockets targeting archipelago servers, supports secure connections", long_about = None)]
struct Cli {
    output: URLPort,
    #[arg(default_value = "38281")]
    input: u32,
}

#[derive(Debug, Clone, Copy)]
struct URLPort {
    url: &'static str,
    port: u32,
}

impl FromStr for URLPort {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err("Invalid format. Expected format: <url>:<port>".to_string());
        }

        let url = parts[0].to_string().leak();
        let port = parts[1].parse::<u32>().map_err(|_| "Invalid port number".to_string())?;

        Ok(URLPort { url, port })
    }
}
use std::str::FromStr;
use std::error::Error;
use std::net::SocketAddr;

use clap::Parser;
use futures_util::{StreamExt, TryStreamExt, future};
use log::{info, warn, debug};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box< dyn Error>> {
    env_logger::init();

    let args = Cli::parse();


    println!("starting proxy on port {} to {}:{}", args.input, args.output.url, args.output.port);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.input)).await?;



    loop {
        let (stream, peer_addr) = listener.accept().await?;
        debug!("accepted TCP connection from {}", peer_addr);

        tokio::spawn(async move {
            handle_conn(stream, peer_addr, args.output).await;
        });

    }
}
async fn handle_conn(stream: TcpStream, peer: SocketAddr, target: URLPort) {
    info!("{}: new connection accepted", peer);
    handle_ws(stream, peer, target).await;
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

    let (client_writer, client_reader) = incoming_connection.split();
    let (target_writer, target_reader) = target_connection.split();

    let mut client_to_target_count: u64 = 0;
    let mut target_to_client_count: u64 = 0;

    let client_to_target = client_reader
        .inspect_ok(|msg| {
            client_to_target_count += 1;
            if client_to_target_count % 100 == 0 {
                debug!("{}: forwarded {} client messages to target", peer, client_to_target_count);
            }
            debug!("{}: client packet: {:?}", peer, msg);
        })
        .try_filter(|msg| future::ready(msg.is_text() || msg.is_binary()))
        .forward(target_writer);

    let target_to_client = target_reader
        .inspect_ok(|msg| {
            target_to_client_count += 1;
            if target_to_client_count % 100 == 0 {
                debug!("{}: forwarded {} target messages to client", peer, target_to_client_count);
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
        Ok(direction) => info!(
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
#[command(author, version, about = "a simple plaintext proxy for websockets targeting archipelago servers", long_about = None)]
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
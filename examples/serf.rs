use futures::StreamExt;
use std::error::Error;

use clap::{Parser, Subcommand};
use log::LevelFilter;
use serf_rpc::protocol::{Member, QueryParams, QueryResponse};
use std::sync::Arc;

const SERF_ADDRESS: &str = "0.0.0.0";
const SERF_PORT: &str = ":7373";

/// Command-line interface for managing Serf interactions.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Host of Serf to connect to
    #[arg(short, long, default_value = "0.0.0.0:7373")]
    address: String,

    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbosity: u8,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List members
    Members,

    /// Check round-trip time
    Rtt {
        /// Name of the member
        destination: String,
    },

    /// Monitor events
    Stream,

    /// Send a query
    Query {
        name: String,
        payload: Option<String>,
    },

    /// Fire an event
    Event {
        name: String,
        payload: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Cli::parse();
    set_log_level(args.verbosity);
    let mut serf_address: String = SERF_ADDRESS.into();
    serf_address.push_str(SERF_PORT);
    let socket = args
        .address
        .parse()
        .expect("Invalid serf IP address provided");
    let client = Arc::new(serf_rpc::Client::connect(socket, None).await?);

    match args.command {
        Commands::Members => {
            let response = client.members().await?;
            for Member {
                name,
                addr,
                port,
                tags,
                status,
                ..
            } in response.members
            {
                let tags: Vec<_> = tags
                    .into_iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect();
                let tags = tags.join(",");
                println!("- {name}  {addr:>15}:{port}  {status}  {tags}");
            }
        }
        Commands::Rtt { destination } => {
            let node_name = client.current_node_name().await?;
            let this_coord = client.get_coordinate(&node_name).await?.coord.unwrap();
            let dest_coord = client.get_coordinate(&destination).await?.coord.unwrap();

            println!(
                "estimated rtt to {destination}: {}s",
                dest_coord.estimate_rtt(&this_coord).as_secs_f32()
            );
        }
        Commands::Stream => {
            let mut stream = client.stream("*");
            while let Some(event) = stream.next().await {
                println!("{:?}", event);
            }
        }
        Commands::Query { name, payload } => {
            let payload = payload.unwrap_or_default();
            let mut response = client.query(&name, payload.as_bytes(), QueryParams::default());
            while let Some(response) = response.next().await {
                match response? {
                    QueryResponse::Done => break,
                    msg => {
                        println!("{msg:?}")
                    }
                }
            }
        }
        Commands::Event { name, payload } => {
            let payload = payload.unwrap_or_default();
            client.fire_event(&name, payload.as_bytes(), true).await?;
        }
    };
    println!("End");
    Ok(())
}

fn set_log_level(verbosity: u8) {
    let log_level = match verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };
    env_logger::Builder::new().filter_level(log_level).init();
}

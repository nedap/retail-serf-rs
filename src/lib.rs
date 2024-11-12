use std::{
    collections::HashMap,
    fmt::Debug,
    net::{SocketAddr, TcpStream},
};

use std::io;
use std::sync::{Arc, Mutex};

use connection::ClientConnection;
use io::BufReader;
use log::{debug};
use protocol::RequestHeader;
use rmpv::decode::read_value;
use serde::de::DeserializeOwned;
const MAX_IPC_VERSION: u32 = 1;

mod connection;
mod coordinates;
mod members;
mod request;
mod stream;

pub mod protocol;

pub use request::RPCRequest;
pub use stream::RPCStream;

/// A wrapper allowing reading a Seq response.
///
/// This is an internal implementation detail, but public because it is exposed in traits.
#[doc(hidden)]
pub struct SeqRead<'a>(&'a mut BufReader<TcpStream>);
impl<'a> SeqRead<'a> {
    fn read_msg<T: DeserializeOwned + Debug>(self) -> T {
        // annoyingly, we pretty much have to panic, because otherwise the reader is left in an invalid state
        let value = read_value(self.0).unwrap();
        debug!("<== {:?}", value);
        T::deserialize(value).unwrap()
    }
}

trait SeqHandler: 'static + Send + Sync {
    fn handle(&self, res: RPCResult<SeqRead>);
    /// are we expecting more than one response?
    fn streaming(&self) -> bool {
        false
    }
    /// is the stream acknowledged ?
    fn stream_acked(&self) -> bool {
        false
    }
}

type RPCResult<T = ()> = Result<T, String>;

#[derive(Clone)]
pub struct Client {
    dispatch: Arc<Mutex<DispatchMap>>,
    tx: std::sync::mpsc::Sender<Vec<u8>>,
}

struct DispatchMap {
    map: HashMap<u64, Arc<dyn SeqHandler>>,
    next_seq: u64,
}

impl DispatchMap {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            next_seq: 0,
        }
    }
}

impl Client {
    /// Connect to hub.
    ///
    /// Waits for handshake, and optionally for authentication if an auth key is provided.
    pub async fn connect(rpc_addr: SocketAddr, auth_key: Option<&str>) -> RPCResult<Self> {
        let (tx, rx) = std::sync::mpsc::channel();
        let dispatch = Arc::new(Mutex::new(DispatchMap::new()));

        ClientConnection::spawn(rpc_addr, rx, dispatch.clone())?;
        let client = Client { dispatch, tx };
        client.handshake(MAX_IPC_VERSION).await?;
        if let Some(auth_key) = auth_key {
            client.auth(auth_key).await?;
        }

        Ok(client)
    }

    fn deregister_seq_handler(&self, seq: u64) -> Option<Arc<dyn SeqHandler>> {
        self.dispatch.lock().unwrap().map.remove(&seq)
    }

    /// Send a command, optionally registering a handler for responses.
    ///
    /// Returns the sequence number.
    fn send_command(&self, cmd: SerializedCommand, handler: Option<Arc<dyn SeqHandler>>) -> u64 {
        let seq = {
            let mut dispatch = self.dispatch.lock().unwrap();

            let seq = dispatch.next_seq;
            dispatch.next_seq += 1;

            if let Some(handler) = handler {
                dispatch.map.insert(seq, handler);
            }

            seq
        };

        let mut buf = rmp_serde::encode::to_vec_named(&RequestHeader {
            command: cmd.name,
            seq,
        })
        .unwrap();
        buf.extend_from_slice(&cmd.body);

        self.tx.send(buf).unwrap();

        seq
    }

    pub async fn current_node_name(&self) -> RPCResult<String> {
        Ok(self.stats().await?.agent.name)
    }
}

struct SerializedCommand {
    name: &'static str,
    body: Vec<u8>,
}

/// A trait for types that can be deserialized as the response to a command
///
/// This is an internal implementation detail, but public because it is exposed in traits.
#[doc(hidden)]
pub trait RPCResponse: Sized + Send + 'static {
    fn read_from(read: SeqRead<'_>) -> RPCResult<Self>;
}

impl RPCResponse for () {
    fn read_from(_: SeqRead<'_>) -> RPCResult<Self> {
        Ok(())
    }
}

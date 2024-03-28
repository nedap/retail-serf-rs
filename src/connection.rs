use crate::protocol::ResponseHeader;
use crate::{DispatchMap, SeqRead};
use io::{BufReader, Write};
use log::{error, info};
use std::error::Error;
use std::fmt::Display;
use std::io;
use std::sync::mpsc::RecvError;
use std::sync::{Arc, Mutex};
use std::{
    net::{SocketAddr, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::task::{JoinError, JoinHandle};

pub(crate) struct ClientConnection {
    dispatch: Arc<Mutex<DispatchMap>>,
    stream: TcpStream,
}

#[derive(Debug)]
enum SerfError {
    Io(io::Error),
    Decode(rmp_serde::decode::Error),
    Receive(RecvError),
    Internal(String),
}

impl Display for SerfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerfError::Io(e) => write!(f, "IO error: {}", e),
            SerfError::Decode(e) => write!(f, "Decode error: {}", e),
            SerfError::Receive(e) => write!(f, "Receive error: {}", e),
            SerfError::Internal(e) => write!(f, "Internal error: {}", e),
        }
    }
}

impl Error for SerfError {}

impl From<io::Error> for SerfError {
    fn from(e: io::Error) -> Self {
        SerfError::Io(e)
    }
}

impl From<rmp_serde::decode::Error> for SerfError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        SerfError::Decode(e)
    }
}

impl From<RecvError> for SerfError {
    fn from(e: RecvError) -> Self {
        SerfError::Receive(e)
    }
}

impl From<JoinError> for SerfError {
    fn from(e: JoinError) -> Self {
        SerfError::Internal(e.to_string())
    }
}

type SerfResult = Result<(), SerfError>;

impl ClientConnection {
    pub(crate) fn spawn(
        rpc_addr: SocketAddr,
        serf_rx: Receiver<Vec<u8>>,
        dispatch: Arc<Mutex<DispatchMap>>,
    ) -> Result<Self, String> {
        info!("Connecting to the Serf instance");
        let stream = TcpStream::connect(rpc_addr).map_err(|e| e.to_string())?;
        info!("Connected to the Serf instance");

        let mut connection = Self {
            dispatch: Arc::clone(&dispatch),
            stream,
        };

        let tx_handle = connection.start_serf_tx(serf_rx)?;
        let rx_handle = connection.start_serf_rx()?;
        connection.start_connection_watcher(tx_handle, rx_handle)?;
        Ok(connection)
    }

    fn start_connection_watcher(
        &mut self,
        tx_handle: JoinHandle<SerfResult>,
        rx_handle: JoinHandle<SerfResult>,
    ) -> Result<std::thread::JoinHandle<()>, String> {
        let dispatch = Arc::clone(&self.dispatch);
        let stream = self.stream.try_clone().map_err(|x| x.to_string())?;
        Ok(std::thread::spawn(move || {
            Self::connection_watcher(dispatch, stream, tx_handle, rx_handle)
        }))
    }

    #[tokio::main(flavor = "current_thread")]
    async fn connection_watcher(
        dispatch: Arc<Mutex<DispatchMap>>,
        stream: TcpStream,
        tx_handle: JoinHandle<SerfResult>,
        rx_handle: JoinHandle<SerfResult>,
    ) {
        let (thread, join_result) = loop {
            tokio::select! {
                res = tx_handle => break ("write", res),
                res = rx_handle => break ("read", res),
            }
        };
        let error = match join_result {
            Ok(Ok(_)) => Err(SerfError::Internal(
                "thread terminated unexpectedly without an error".to_string(),
            )),
            Ok(error) => error,
            Err(e) => Err(e.into()),
        };

        error!("Error in connection {thread} thread: {error:?}");

        info!("Cleaning up Serf threads");
        drop(stream);

        info!("Cleaning up existing Serf requests by responding with an error");
        for (_, value) in dispatch.lock().unwrap().map.drain() {
            value.handle(Err(
                "Request was cancelled due to an error in the SERF connection".to_string(),
            ));
        }
    }

    fn start_serf_tx(
        &mut self,
        rx_rw: Receiver<Vec<u8>>,
    ) -> Result<JoinHandle<SerfResult>, String> {
        let mut stream = self.stream.try_clone().map_err(|x| x.to_string())?;
        Ok(tokio::task::spawn_blocking(move || loop {
            stream.write_all(&rx_rw.recv()?)?;
        }))
    }

    fn start_serf_rx(&mut self) -> Result<JoinHandle<SerfResult>, String> {
        let dispatch = Arc::clone(&self.dispatch);
        let mut reader = BufReader::new(self.stream.try_clone().map_err(|x| x.to_string())?);
        Ok(tokio::task::spawn_blocking(move || loop {
            let ResponseHeader { seq, error } = rmp_serde::from_read(&mut reader)?;
            let mut dispatch = dispatch.lock().unwrap();
            if let Some(v) = dispatch.map.get(&seq) {
                let seq_handler = if v.streaming() {
                    if !v.stream_acked() {
                        continue;
                    }
                    v.clone()
                } else {
                    dispatch.map.remove(&seq).unwrap()
                };

                seq_handler.handle(if error.is_empty() {
                    Ok(SeqRead(&mut reader))
                } else {
                    Err(error)
                });
            }
        }))
    }
}

use std::{
    net::{SocketAddr, TcpStream},
    sync::{atomic::AtomicBool, mpsc::Receiver},
    thread::JoinHandle,
};

use std::io;
use std::sync::{Arc, Mutex};

use io::{BufReader, Write};
use log::{error, info};

pub use crate::request::RPCRequest;
pub use crate::stream::RPCStream;
use crate::{DispatchMap, SeqRead};
use tokio::sync::Notify;

pub(crate) struct ClientConnection {
    notifier: Arc<Notify>,
    cancel: Arc<AtomicBool>,
    dispatch: Arc<Mutex<DispatchMap>>,
    stream: TcpStream,
}

impl ClientConnection {
    fn start_serf_tx(
        &mut self,
        rx_rw: std::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<JoinHandle<()>, String> {
        let notifier = Arc::clone(&self.notifier);
        let cancel = Arc::clone(&self.cancel);
        let mut stream = self.stream.try_clone().map_err(|x| x.to_string())?;
        Ok(std::thread::spawn(move || loop {
            match rx_rw.recv() {
                Ok(x) => {
                    if let Err(e) = stream.write_all(&x) {
                        error!("Error while writing to serf stream: {e}");
                        cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                        notifier.notify_one();
                        return;
                    }
                }
                Err(e) => {
                    error!("Error while receiving value from channel: {e}");
                    cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                    notifier.notify_one();
                    return;
                }
            }
        }))
    }

    fn start_serf_rx(&mut self) -> Result<JoinHandle<()>, String> {
        let notifier = Arc::clone(&self.notifier);
        let cancel = Arc::clone(&self.cancel);
        let dispatch = Arc::clone(&self.dispatch);
        let mut reader = BufReader::new(self.stream.try_clone().map_err(|x| x.to_string())?);
        Ok(std::thread::spawn(move || {
            loop {
                let crate::protocol::ResponseHeader { seq, error } =
                    match rmp_serde::from_read(&mut reader) {
                        Ok(x) => x,
                        Err(e) => {
                            error!("Error while reading messagepack: {e}");
                            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
                            notifier.notify_one();
                            return;
                        }
                    };

                let seq_handler = {
                    let mut dispatch = dispatch.lock().unwrap();
                    match dispatch.map.get(&seq) {
                        Some(v) => {
                            if v.streaming() {
                                if !v.stream_acked() {
                                    continue;
                                }
                                v.clone()
                            } else {
                                dispatch.map.remove(&seq).unwrap()
                            }
                        }
                        None => {
                            // response with no handler, ignore
                            continue;
                        }
                    }
                };

                let res = if error.is_empty() {
                    Ok(SeqRead(&mut reader))
                } else {
                    Err(error)
                };

                seq_handler.handle(res);
            }
        }))
    }

    #[tokio::main(flavor = "current_thread")]
    async fn connection_watcher(
        dispatch: Arc<Mutex<DispatchMap>>,
        cancel: Arc<AtomicBool>,
        thread_notifier: Arc<Notify>,
        stream: TcpStream,
        serf_tx_handle: JoinHandle<()>,
        serf_rx_handle: JoinHandle<()>,
    ) {
        while !cancel.load(std::sync::atomic::Ordering::Relaxed) {
            info!("Waiting until connection threads send notification");
            thread_notifier.notified().await;
        }

        info!("Cleaning up SERF threads");
        drop(stream);

        info!("Cleaning up existing SERF requests by responding with an error");
        let mut lock = dispatch.lock().unwrap();
        let map = &mut lock.map;
        for (_, value) in map.iter() {
            value.handle(Err(
                "Request was cancelled due to an error in the SERF connection".to_string(),
            ));
        }
        map.clear();
        drop(lock);

        if let Err(e) = serf_tx_handle.join() {
            error!("Error in connection write thread: {e:?}");
        }
        if let Err(e) = serf_rx_handle.join() {
            error!("Error in connection read thread: {e:?}");
        }
    }

    fn start_connection_watcher(
        &mut self,
        serf_tx_handle: JoinHandle<()>,
        serf_rx_handle: JoinHandle<()>,
    ) -> Result<JoinHandle<()>, String> {
        let notifier = Arc::clone(&self.notifier);
        let cancel = Arc::clone(&self.cancel);
        let dispatch = Arc::clone(&self.dispatch);
        let stream = self.stream.try_clone().map_err(|x| x.to_string())?;
        Ok(std::thread::spawn(move || {
            Self::connection_watcher(
                dispatch,
                cancel,
                notifier,
                stream,
                serf_tx_handle,
                serf_rx_handle,
            )
        }))
    }

    pub(crate) fn spawn(
        rpc_addr: SocketAddr,
        serf_rx: Receiver<Vec<u8>>,
        dispatch: Arc<Mutex<DispatchMap>>,
    ) -> Result<Self, String> {
        info!("Connecting to the Serf instance");
        let stream = TcpStream::connect(rpc_addr).map_err(|e| e.to_string())?;
        info!("Connected to the Serf instance");

        let mut connection = Self {
            notifier: Arc::new(Notify::new()),
            cancel: Arc::new(AtomicBool::new(false)),
            dispatch: Arc::clone(&dispatch),
            stream,
        };

        let tx_handle = connection.start_serf_tx(serf_rx)?;
        let rx_handle = connection.start_serf_rx()?;
        connection.start_connection_watcher(tx_handle, rx_handle)?;
        Ok(connection)
    }
}

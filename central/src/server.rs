use std::sync::{Arc};
use std::time::Duration;
use log::{debug, info};
use tokio::sync::Mutex;
use mbei_core::event::Update;
use mbei_grpc::process_update_server::{await_server_handle_with_timeout, create_and_run_server, Queue};
use crate::Central;

pub(crate) struct CentralServer {
    grpc_port: u16,
    pub(crate) central: Central,
}

impl CentralServer {
    pub(crate) fn new(grpc_port:u16, central:Central) -> CentralServer {
        CentralServer { grpc_port, central }
    }

    pub(crate) async fn run(&self) {
        let (new_update_sender, mut new_update_receiver) = tokio::sync::mpsc::unbounded_channel();
        let queue_mutex = Mutex::new(Queue::new());
        let arc_queue_mutex = Arc::new(queue_mutex);
        let (shutdown_server_sender, shutdown_server_receiver) = tokio::sync::oneshot::channel();
        let server_handle = create_and_run_server(self.grpc_port, arc_queue_mutex.clone(), new_update_sender, shutdown_server_receiver).await;

        loop {
            let update_opt;
            {
                let mut queue = arc_queue_mutex.lock().await;
                update_opt = queue.pop_earliest_update();
            }
            if let Some(update) = update_opt {
                match update {
                    Update::Stop => {
                        shutdown_server_sender.send(()).expect("Shutdown error");
                        info!("Received stop update, stopping.");
                        break;
                    }
                    nonstop_update => {
                        self.central.process_update(nonstop_update);
                    }
                }
            } else {
                new_update_receiver.recv().await;
            }
        }
        debug!("Almost shut down, wait for server handle");
        await_server_handle_with_timeout(server_handle, Duration::from_secs(5)).await;
        debug!("Shut down");
    }

    pub fn close(self) {
        self.central.conn.close().expect("Problem closing database");
    }
}
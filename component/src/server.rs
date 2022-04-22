use crate::Component;
use log::{debug, info};
use mbei_core::event::Update;
use mbei_grpc::process_update::ProcessUpdateResponse;
use mbei_grpc::process_update_server::{
    await_server_handle_with_timeout, create_and_run_server, Queue,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::Status;

type JoinHandleType = JoinHandle<Result<tonic::Response<ProcessUpdateResponse>, Status>>;

pub struct ComponentServer {
    query_name: String,
    component: Component,
    grpc_port: u16,
    handle_sender: UnboundedSender<JoinHandleType>,
}

impl ComponentServer {
    pub fn new(
        query_name: String,
        component: Component,
        grpc_port: u16,
        handle_sender: UnboundedSender<
            JoinHandle<Result<tonic::Response<ProcessUpdateResponse>, Status>>,
        >,
    ) -> ComponentServer {
        ComponentServer {
            query_name,
            component,
            grpc_port,
            handle_sender,
        }
    }

    pub async fn run(&mut self, max_elapsed_time: Option<Duration>) {
        let (new_update_sender, mut new_update_receiver) = tokio::sync::mpsc::unbounded_channel();
        let queue_mutex = Mutex::new(Queue::new());
        let arc_queue_mutex = Arc::new(queue_mutex);
        let (shutdown_server_sender, shutdown_server_receiver) = tokio::sync::oneshot::channel();
        let server_handle = create_and_run_server(
            self.grpc_port,
            arc_queue_mutex.clone(),
            new_update_sender,
            shutdown_server_receiver,
        )
        .await;
        info!(
            "{} component server started and is ready to serve",
            &self.query_name
        );
        self.component.start(max_elapsed_time).await;
        info!(
            "{} component started and is ready to serve",
            &self.query_name
        );
        loop {
            let update_opt;
            {
                let mut queue = arc_queue_mutex.lock().await;
                update_opt = queue.pop_earliest_update();
                info!(
                    "{} has queue lengths retractions: {}, deltas: {}, events: {}",
                    &self.query_name,
                    &queue.open_retractions.len(),
                    &queue.open_deltas.len(),
                    &queue.open_events.len()
                );
            }
            if let Some(update) = update_opt {
                let now = Instant::now();
                match update {
                    Update::Stop => {
                        self.component.stop();
                        shutdown_server_sender
                            .send(())
                            .expect("Error sending shutdown");
                        info!("{} received stop update, stopping.", &self.query_name);
                        break;
                    }
                    non_stop_update => {
                        info!("{} received update", &self.query_name);
                        let handles = self
                            .component
                            .process_update_until_consistency(non_stop_update)
                            .await;
                        for h in handles {
                            self.handle_sender.send(h).expect("Error sending handle");
                        }
                    }
                }
                info!(
                    "{} message processing took {} μs",
                    &self.query_name,
                    now.elapsed().as_micros()
                );
            } else {
                new_update_receiver.recv().await;
            }
        }
        debug!("Almost shut down, waiting for server handle");
        await_server_handle_with_timeout(server_handle, Duration::from_secs(5)).await;
        debug!("Shut down");
    }
}

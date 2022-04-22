use crate::process_update::process_update_client::ProcessUpdateClient;
use backoff::ExponentialBackoffBuilder;
use log::{debug, info};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tonic::Status;
use tonic::transport::Channel;
use crate::process_update::ProcessUpdateResponse;

type JoinHandleType = JoinHandle<Result<tonic::Response<ProcessUpdateResponse>, Status>>;

pub async fn create_process_update_client(
    url: String,
    max_elapsed_time: Option<Duration>,
) -> Option<ProcessUpdateClient<Channel>> {
    debug!("Process update client with url {} starting", url);
    let op = || async {
        let client = ProcessUpdateClient::connect(url.to_string()).await?;
        Ok(client)
    };

    let backoff = ExponentialBackoffBuilder::new()
        .with_max_elapsed_time(max_elapsed_time)
        .with_max_interval(Duration::from_secs(10))
        .build();
    let client = Some(
        backoff::future::retry(backoff, op)
            .await
            .expect("Could not connect to backend grpc service in maximum allotted time"),
    );
    debug!("Process update client with url {} started", url);
    client
}

pub async fn await_deliveries(mut receiver: UnboundedReceiver<JoinHandleType>) {
    loop {
        let handle_opt = receiver.recv().await;
        if let Some(handle) = handle_opt {
            handle.await.expect("Error sending").expect("Error sending");
        } else {
            break;
        }
    }
}

pub async fn await_deliveries_max_queue(mut receiver: UnboundedReceiver<JoinHandleType>, stop_sender:Sender<()>, max_queue_size: u32) {
    loop {
        let handle_opt = receiver.recv().await;
        if let Some(handle) = handle_opt {
            let resp = handle.await.expect("Error sending").expect("Error sending").into_inner();
            if resp.queue_size > max_queue_size {
                info!("Stopped as max queue size was reached");
                stop_sender.send(()).expect("Error sending");
                break;
            }
        } else {
            break;
        }
    }
}
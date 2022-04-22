use std::net::SocketAddr;
use std::sync::{Arc};
use std::time::Duration;
use tonic::{Request, Response, Status};
use mbei_core::event::{Deltas, Event, Retractions, Update};
use tokio::sync::mpsc::{UnboundedSender};
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinHandle;
use tonic::transport::{Error, Server};
use crate::process_update::{ProcessUpdateRequest, ProcessUpdateResponse};
use crate::process_update::process_update_server::{ProcessUpdate, ProcessUpdateServer};
use crate::process_update_mapping::update_from_request;
use futures_util::FutureExt;
use log::warn;
use tokio::sync::Mutex;

pub async fn create_and_run_server(grpc_port:u16, arc_queue_mutex: Arc<Mutex<Queue>>, new_update_sender:UnboundedSender<()>, shutdown_server_receiver: Receiver<()>) -> JoinHandle<Result<(), Error>> {
    let service = ProcessUpdateService {
            sender: Mutex::new(new_update_sender),
            queue: arc_queue_mutex,
    };
    let address_string = "[::]:".to_owned() + &grpc_port.to_string();
    let address: SocketAddr = address_string
        .parse()
        .expect("Error parsing server address");
    let svc = ProcessUpdateServer::new(service);
    let server_handle = tokio::spawn(
        Server::builder().add_service(svc).serve_with_shutdown(address, shutdown_server_receiver.map(|_| ()))
    );
    server_handle
}

pub async fn await_server_handle_with_timeout(server_handle:JoinHandle<Result<(), Error>>, timeout:Duration) {
    let server_result = tokio::time::timeout(timeout, server_handle).await;
    match server_result {
        Ok(r) => {r.expect("Error serving").expect("Error serving");}
        Err(_) => {warn!("Server did not stop gracefully, timeout reached");}
    }
}

pub struct ProcessUpdateService {
    pub sender: Mutex<UnboundedSender<()>>,
    pub queue: Arc<Mutex<Queue>>,
}

#[tonic::async_trait]
impl ProcessUpdate for ProcessUpdateService {
    async fn send(
        &self,
        request: Request<ProcessUpdateRequest>,
    ) -> Result<Response<ProcessUpdateResponse>, Status> {
        let new_update = update_from_request(request.get_ref());
        let queue_size;
        {
            let mut q = self.queue.lock().await;
            q.insert_update(new_update);
            queue_size = q.get_queue_size() as u32;
        }
        {
            let s = self.sender.lock().await;
            s.send(()).expect("Error sending new update message");
        }
        Ok(Response::new(ProcessUpdateResponse {
            queue_size
        }))
    }
}

pub struct Queue {
    pub open_events: Vec<Event>,
    pub open_deltas: Vec<Deltas>,
    pub open_retractions: Vec<Retractions>,
    pub stop: bool,

}

impl Queue {
    pub fn new() -> Queue{
        Queue {
            open_events: vec![],
            open_deltas: vec![],
            open_retractions: vec![],
            stop: false
        }
    }

    fn insert_update(&mut self, update: Update) {
        match update {
            Update::Stop => {self.stop = true;}
            Update::Event(e) => {
                self.open_events.push(e);
            }
            Update::Deltas(ds) => {
                self.open_deltas.push(ds);
            }
            Update::Retractions(rs) => {
                self.open_retractions.push(rs);
            }
        };
    }

    fn get_queue_size(&self) -> usize {
        self.open_events.len() + self.open_deltas.len() + self.open_retractions.len()
    }

    pub fn pop_earliest_update(&mut self) -> Option<Update> {
        if self.stop {
            Some(Update::Stop)
        } else if !self.open_retractions.is_empty() {
            let (min_index, _) = self.open_retractions.iter().enumerate().min_by_key(|(_, r)|r.timestamp).unwrap();
            let retractions = self.open_retractions.swap_remove(min_index);
            Some(Update::Retractions(retractions))
        } else if !self.open_deltas.is_empty() {
            let (min_index, _) = self.open_deltas.iter().enumerate().min_by_key(|(_, r)|r.origin_timestamp).unwrap();
            let deltas = self.open_deltas.swap_remove(min_index);
            Some(Update::Deltas(deltas))
        } else if !self.open_events.is_empty() {
            let (min_index, _) = self.open_events.iter().enumerate().min_by_key(|(_, r)|r.timestamp).unwrap();
            let event = self.open_events.swap_remove(min_index);
            Some(Update::Event(event))
        }
        else {
            None
        }

    }
}

#[test]
fn test_queue() {
    let mut queue = Queue::new();
    let e1 = Update::Event(Event {
        event_id: "abc".to_string(),
        timestamp: 3,
        node_id: "abc123".to_string(),
        payload: vec![]
    });
    let d1 = Update::Deltas(Deltas{
        deltas_id: "".to_string(),
        origin_id: "".to_string(),
        origin_timestamp: 99,
        deltas: Default::default()
    });
    let e2 = Update::Event(Event {
        event_id: "abc".to_string(),
        timestamp: 4,
        node_id: "abc123".to_string(),
        payload: vec![]
    }).clone();
    let r1 = Update::Retractions(Retractions {
        retraction_id: "r3".to_string(),
        timestamp: 5,
        deltas_ids: vec![]
    });
    queue.insert_update(d1.clone());
    queue.insert_update(e1.clone());
    queue.insert_update(e2.clone());
    queue.insert_update(r1.clone());
    let u1 = queue.pop_earliest_update();
    let u2 = queue.pop_earliest_update();
    let u3 = queue.pop_earliest_update();
    let u4 = queue.pop_earliest_update();
    let u5 = queue.pop_earliest_update();

    assert_eq!(u1.unwrap(), r1);
    assert_eq!(u2.unwrap(), d1);
    assert_eq!(u3.unwrap(), e1);
    assert_eq!(u4.unwrap(), e2);
    assert_eq!(u5, None)
}
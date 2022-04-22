use std::collections::{BTreeMap, BTreeSet};
use std::thread::sleep;
use std::time::{Duration, Instant};

use log::{debug, info};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tonic::{Response, Status};
use tonic::transport::Channel;
use uuid::Uuid;
use serde::{Deserialize, Serialize};

use mbei_core::event::{Deltas, Event, Update};
use mbei_core::graph::{Delta, DeltaType, Node};
use mbei_grpc::process_update_client::{await_deliveries, await_deliveries_max_queue};
use mbei_grpc::process_update::process_update_client::ProcessUpdateClient;
use mbei_grpc::process_update::{ProcessUpdateRequest, ProcessUpdateResponse};
use mbei_grpc::process_update_client::create_process_update_client;
use mbei_grpc::process_update_mapping::request_from_update;
use crate::message_creator::MessageCreator;

type JoinHandleType = JoinHandle<Result<tonic::Response<ProcessUpdateResponse>, Status>>;

#[derive(Clone)]
pub struct TestdataProducer {
    client_map: BTreeMap<String, ProcessUpdateClient<tonic::transport::Channel>>,
    use_central: bool
}

impl TestdataProducer {
    pub async fn new(query_url_map: BTreeMap<String, String>, max_elapsed_time: Option<Duration>, use_central:bool) -> TestdataProducer {
        let mut client_map = BTreeMap::new();
        for (q, url) in query_url_map {
            client_map.insert(q, create_process_update_client(url, max_elapsed_time).await.expect("Problem creating client"));
        }
        TestdataProducer {
            client_map,
            use_central
        }
    }

    pub async fn send_messages_in_vec_until_max_queue(&self, messages:Vec<TopicNameAndUpdate>, max_queue_size:u32, init_messages_per_second:u32, step:u32, every:u32) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
        let receiver_handle = tokio::spawn(await_deliveries_max_queue(receiver, stop_sender, max_queue_size));

        let mut messages_per_second = init_messages_per_second;
        let mut seq = 0;
        let mut at_last_1000 = Instant::now();
        for m in messages {
            if let Ok(()) = stop_receiver.try_recv() {
                info!("Stopping due to queue size backpressure at {} messages per second", &messages_per_second);
                break;
            }
            if seq > 0 && seq % every == 0 {
                info!("{} messages per second had actual rate {} messages per second", &messages_per_second, 1000.0 / at_last_1000.elapsed().as_secs_f64());
                messages_per_second += step;
                info!("Messages per second set to: {}", &messages_per_second);
                at_last_1000 = Instant::now();
            }
            let handle = self.send_update(&m.update, m.topic_name).await;
            sender.send(handle).expect("Error sending");
            delay(messages_per_second);
            seq += 1;
        }
        drop(sender);
        receiver_handle.await.expect("Error in receiver");
    }

    pub async fn send_reversed_messages_in_vec_until_max_queue(&self, messages:Vec<TopicNameAndUpdate>, max_queue_size:u32, init_messages_per_second:u32, step:u32, every:u32) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let (stop_sender, mut stop_receiver) = tokio::sync::oneshot::channel();
        let receiver_handle = tokio::spawn(await_deliveries_max_queue(receiver, stop_sender, max_queue_size));

        let mut messages_per_second = init_messages_per_second;
        let mut seq = 0;
        let mut at_last_1000 = Instant::now();
        'outer: for i in 0..((messages.len()/10)-1) {
            let batch = &messages[i * 10..(i + 1) * 10];
            for j in (0..batch.len()).rev() {
                let m = batch.get(j).unwrap();
                if let Ok(()) = stop_receiver.try_recv() {
                    info!("Stopping due to queue size backpressure at {} messages per second", &messages_per_second);
                    break 'outer;
                }
                if seq > 0 && seq % every == 0 {
                    info!("{} messages per second had actual rate {} messages per second", &messages_per_second, 1000.0 / at_last_1000.elapsed().as_secs_f64());
                    messages_per_second += step;
                    info!("Messages per second set to: {}", &messages_per_second);
                    at_last_1000 = Instant::now();
                }
                let handle = self.send_update(&m.update, m.topic_name.clone()).await;
                sender.send(handle).expect("Error sending");
                delay(messages_per_second);
                seq += 1;
            }
        }
        drop(sender);
        receiver_handle.await.expect("Error in receiver");
    }

    pub async fn send_stop_now(&self, topic_name: &str) {
        let handle = self.send_stop(topic_name).await;
        handle.await.expect("Sending failed").expect("Sending failed");
    }

    pub async fn send_stop(&self, topic_name: &str) -> JoinHandleType {
        let update = Update::Stop;
        self.send_update(&update, topic_name.to_string()).await
    }

    pub async fn send_event_now(&self, topic_name: &str, event: Event) {
        let handle = self.send_event(topic_name, event).await;
        handle.await.expect("Sending failed").expect("Sending failed");
    }

    pub async fn send_event(&self, topic_name: &str, event: Event) -> JoinHandleType{
        let update = Update::Event(event);
        self.send_update(&update, topic_name.to_string()).await
    }

    pub async fn send_deltas_now(&self, deltas_id: &str, topic_name: &str, deltas: Vec<Delta>) {
        let handles = self.send_deltas(
            deltas_id, topic_name, deltas
        ).await;
        for h in handles {
            h.await.expect("Sending failed").expect("Sending failed");
        }
    }

    pub async fn send_deltas(&self, deltas_id: &str, topic_name: &str, deltas: Vec<Delta>) -> Vec<JoinHandleType> {
        let min_ts = deltas.iter().min_by_key(|d| d.timestamp).unwrap().timestamp;
        let update = Update::Deltas(Deltas{
            deltas_id: deltas_id.to_string(),
            origin_id: deltas_id.to_string(),
            origin_timestamp: min_ts,
            deltas: BTreeSet::from_iter(deltas.into_iter()),
        }) ;
        let mut handles = vec![];
        if self.use_central {
            let central_handle = self.send_update(&update, "central".to_string()).await;
            handles.push(central_handle);
        }
        let component_handle = self.send_update(&update, topic_name.to_string()).await;
        handles.push(component_handle);
        handles
    }

    pub async fn send_update(&self, update: &Update, topic_name: String) -> JoinHandleType {
        debug!("Sending update to {}",&topic_name);
        let client = self.client_map.get(&topic_name).unwrap().clone();
        let request = request_from_update(update);

        let handle = tokio::spawn(TestdataProducer::owning_send(client, request
        ));
        handle
    }

    async fn owning_send(mut client:ProcessUpdateClient<Channel>, request:ProcessUpdateRequest) -> Result<Response<ProcessUpdateResponse>, Status> {
        client.send(request).await
    }

    pub async fn produce_until_interrupted_or_n_reached(
        &mut self,
        message_creator: &mut dyn MessageCreator,
        messages_per_second: u32,
        n: Option<u32>,
    ) -> u64 {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let receiver_handle = tokio::spawn(await_deliveries(receiver));

        let (ctrl_c_sender, mut ctrl_c_receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(loop_to_send_stop_if_ctrl_c_received(ctrl_c_sender));

        let mut stopping = false;
        let mut seq: u32 = 0;
        loop {
            if !stopping {
                if let Ok(()) = ctrl_c_receiver.try_recv() {
                    stopping = true;
                    info!("Flushing barrels, as there are still some left");
                }
                if let Some(max_seq) = n {
                    if max_seq <= seq {
                        stopping = true;
                        info!("Reached max seq, stopping");
                    }
                }
            }
            if stopping && message_creator.is_finished() {
                info!("All barrels are flushed");
                break;
            }
            let (_, topic_name_and_update) = message_creator.create_one_new_message(stopping);
            for tu in topic_name_and_update {
                let h = self.send_update(&tu.update, tu.topic_name).await;
                sender
                    .send(h)
                    .expect("Error sending handle to receiver task");
            }
            seq += 1;
            delay(messages_per_second);
        }
        drop(sender);
        receiver_handle.await.expect("Problem producing");
        message_creator.get_current_timestamp()
    }
}

pub fn create_single_delta_update(
        src: Node,
        trg: Node,
        edge_type: &str,
        timestamp: u64,
        delta_type: DeltaType,
    ) -> Update {
        let id = Uuid::new_v4().to_hyphenated().to_string();
        Update::Deltas(Deltas{
            deltas_id: id.clone(),
            origin_id: id,
            origin_timestamp: timestamp,
            deltas: BTreeSet::from([Delta {
                src: src,
                trg: trg,
                edge_type: edge_type.to_string(),
                timestamp: timestamp,
                delta_type: delta_type,
            }]),
        })
    }

pub(crate) async fn loop_to_send_stop_if_ctrl_c_received(sender: Sender<()>) {
    tokio::signal::ctrl_c()
        .await
        .expect("Listening for ctrl c error");
    info!("Got ctrl-c, stopping gracefully");
    sender.send(()).expect("Problem sending");
}

pub(crate) fn delay(messages_per_second: u32) {
    if messages_per_second > 0 {
        sleep(Duration::from_secs_f64(1.0 / (messages_per_second as f64)));
    }
}

#[derive(Serialize, Deserialize)]
pub struct TopicNameAndUpdate {
    pub topic_name:String,
    pub update: Update
}

impl TopicNameAndUpdate {
    pub fn new(topic_name:String, update:Update) -> TopicNameAndUpdate {
        TopicNameAndUpdate {topic_name, update}
    }
}
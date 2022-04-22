/*Copyright 2022 Prediktor AS

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

use crate::common::application_component::{
    ApplicationRequest, ApplicationResponse, InstanceEdge, Match, QueryEdge,
};
use crate::common::delta::{Delta, DeltaType, InstanceNode, NodeClass, OptionalBytes};

use crate::common::empty_response;
use bincode::{config::Configuration, Decode, Encode};
use log::{debug, warn};
use tonic::{Response, Status};

#[derive(Decode, Encode, Debug)]
pub struct StampEvent {
    pub stamp_data: String,
}

pub(crate) struct StampApplication {
    config: Configuration,
}

impl StampApplication {
    pub(crate) fn new() -> StampApplication {
        StampApplication {
            config: bincode::config::standard(),
        }
    }

    pub(crate) fn stamp_function(
        &self,
        request: &ApplicationRequest,
    ) -> Result<Response<ApplicationResponse>, Status> {
        match &request.event {
            Some(event) => {
                let (stamp_event, _): (StampEvent, usize) =
                    bincode::decode_from_slice(&event.payload, self.config)
                        .expect("Error decoding");
                self.process_stamp_event(
                    event.timestamp,
                    stamp_event,
                    &request.matches,
                    &request.query_graph,
                )
            }
            None => {
                warn!("Event missing from request");
                empty_response()
            }
        }
    }

    fn process_stamp_event(
        &self,
        timestamp: u64,
        stamp_event: StampEvent,
        matches: &Vec<Match>,
        _query: &Vec<QueryEdge>,
    ) -> Result<Response<ApplicationResponse>, Status> {
        let mut barrel_at_stamp: Option<InstanceEdge> = None;
        debug!("Matches {:?}", &matches);
        for m in matches {
            for t in &m.tuples {
                if t.src.as_ref().unwrap().edge_type == "At" {
                    if t.trg.as_ref().is_none() {
                        debug!("Empty match");
                        return empty_response();
                    }
                    if barrel_at_stamp.is_some()
                        && barrel_at_stamp.as_ref().unwrap().from_timestamp
                            < t.trg.as_ref().unwrap().from_timestamp
                    {
                        barrel_at_stamp = t.trg.clone();
                    }
                    if barrel_at_stamp.is_none() {
                        barrel_at_stamp = t.trg.clone();
                    }
                }
            }
        }
        if barrel_at_stamp.is_none() {
            debug!("No matches are found, so there is no change.");
            return empty_response();
        }
        let barrel_at_stamp = barrel_at_stamp.unwrap();
        if barrel_at_stamp.src.as_ref().is_none()
            || barrel_at_stamp.src.as_ref().unwrap().node_type != "Barrel"
        {
            debug!("Wrong kind of material at stamp");
            return empty_response();
        }
        let payload =
            bincode::encode_to_vec(stamp_event.stamp_data, self.config).expect("Encoding failed");
        let trg_node = create_stamp_node(
            barrel_at_stamp
                .src
                .as_ref()
                .unwrap()
                .instance_node_id
                .clone(),
            timestamp,
            payload,
        );
        let stamped_barrel_delta = Delta {
            src: barrel_at_stamp.src.clone(),
            trg: Some(trg_node),
            edge_type: "HasStampData".to_string(),
            timestamp,
            delta_type: DeltaType::Addition as i32,
        };

        Ok(Response::new(ApplicationResponse {
            deltas: vec![stamped_barrel_delta],
        }))
    }
}

pub fn create_stamp_node(
    instance_node_id: String,
    timestamp: u64,
    payload: Vec<u8>,
) -> InstanceNode {
    let instance_node_id = instance_node_id + "_Stamp_" + &timestamp.to_string();
    InstanceNode {
        instance_node_id,
        node_type: "StampData".to_string(),
        node_class: NodeClass::Property as i32,
        value: Some(OptionalBytes { b: payload }),
    }
}

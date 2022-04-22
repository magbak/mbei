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
    ApplicationRequest, ApplicationResponse, Match,
    QueryEdge};
use crate::common::delta::{Delta, DeltaType};
use crate::common::{as_instance_node, empty_response};

use bincode::{config::Configuration, Decode, Encode};
use tonic::{Response, Status};
use log::{debug, info, warn};

#[derive(Decode, Encode, Debug)]
pub enum CraneEventType {
    PickUp,
    Drop,
}

#[derive(Decode, Encode, Debug)]
pub struct CraneEvent {
    pub instance_node_id: String,
    pub crane_event_type: CraneEventType,
}

pub(crate) struct CraneApplication {
    config: Configuration,
}

impl CraneApplication {
    pub(crate) fn new() -> CraneApplication {
        CraneApplication {
            config: bincode::config::standard(),
        }
    }

    pub(crate) fn crane_function(
        &self,
        request: &ApplicationRequest,
    ) -> Result<Response<ApplicationResponse>, Status> {
        debug!("Request: {:?}", &request);
        match &request.event {
            Some(event) => {
                let (crane_event, _): (CraneEvent, usize) =
                    bincode::decode_from_slice(&event.payload, self.config)
                        .expect("Error decoding");
                let resp = CraneApplication::process_crane_event(
                    event.timestamp,
                    crane_event,
                    &request.matches,
                    &request.query_graph,
                );
                debug!("Response: {:?}", &resp);
                resp
            }
            _ => {
                warn!("Event missing from request");
                empty_response()
            }
        }
    }

    fn process_crane_event(
        timestamp: u64,
        crane_event: CraneEvent,
        matches: &Vec<Match>,
        query_graph: &Vec<QueryEdge>,
    ) -> Result<Response<ApplicationResponse>, Status> {
        match crane_event.crane_event_type {
            CraneEventType::PickUp => CraneApplication::process_pick_up_event(
                timestamp,
                crane_event,
                matches,
                query_graph,
            ),
            CraneEventType::Drop => CraneApplication::process_drop_event(
                timestamp,
                crane_event,
                matches,
                query_graph,
            ),
        }
    }

    fn process_pick_up_event(
        timestamp: u64,
        crane_event: CraneEvent,
        matches: &Vec<Match>,
        query_graph: &Vec<QueryEdge>,
    ) -> Result<Response<ApplicationResponse>, Status> {
        let mut crane = None;
        for edge in query_graph {
            if edge.src.as_ref().unwrap().node_type.as_ref().unwrap().s == "Crane" {
                crane = edge.src.as_ref().clone();
                break;
            }
        }
        if crane.is_none() {
            warn!("Could not find crane node");
            return empty_response();
        }
        let crane = crane.unwrap();

        let mut found_barrel_at_node_tuple = None;

        for m in matches {
            for match_tuple in &m.tuples {
                if match_tuple
                    .trg
                    .as_ref()
                    .unwrap()
                    .trg
                    .as_ref()
                    .unwrap()
                    .instance_node_id
                    == crane_event.instance_node_id
                {
                    if found_barrel_at_node_tuple.is_none() {
                        found_barrel_at_node_tuple = Some(match_tuple)
                    } else if found_barrel_at_node_tuple
                        .unwrap()
                        .trg
                        .as_ref()
                        .unwrap()
                        .from_timestamp < match_tuple
                            .trg
                            .as_ref()
                            .unwrap()
                            .from_timestamp
                    {
                        found_barrel_at_node_tuple = Some(match_tuple)
                    }
                }
            }
        }
        if found_barrel_at_node_tuple.is_none() {
            info!(
                "Could not find match that corresponded to instance in event, no node at barrel?"
            );
            debug!(
                "Could not find barrel at {:?} crane among matches {:?}, returning empty response",
                &crane_event.instance_node_id, &matches
            );
            return empty_response();
        }
        let barrel_at_node_tuple = found_barrel_at_node_tuple.unwrap();
        let barrel_removed_from_node = Delta {
            src: barrel_at_node_tuple.trg.as_ref().unwrap().src.clone(),
            trg: barrel_at_node_tuple.trg.as_ref().unwrap().trg.clone(),
            edge_type: barrel_at_node_tuple.trg.as_ref().unwrap().edge_type.clone(),
            timestamp: timestamp,
            delta_type: DeltaType::Removal as i32,
        };
        let barrel_added_to_crane = Delta {
            src: barrel_at_node_tuple.trg.as_ref().unwrap().src.clone(),
            trg: Some(as_instance_node(crane)),
            edge_type: barrel_at_node_tuple.trg.as_ref().unwrap().edge_type.clone(),
            timestamp: timestamp + 1,
            delta_type: DeltaType::Addition as i32,
        };
        let deltas = vec![barrel_removed_from_node, barrel_added_to_crane];
        debug!("Successfully processed pickup event, returning deltas");
        Ok(Response::new(ApplicationResponse { deltas }))
    }

    fn process_drop_event(
        timestamp: u64,
        crane_event: CraneEvent,
        matches: &Vec<Match>,
        query_graph: &Vec<QueryEdge>,
    ) -> Result<Response<ApplicationResponse>, Status> {
        let mut object = None;
        for edge in query_graph {
            if edge
                .trg
                .as_ref()
                .unwrap()
                .instance_node_id
                .as_ref()
                .unwrap()
                .s
                == crane_event.instance_node_id
            {
                object = edge.trg.clone();
                break;
            }
        }
        if object.is_none() {
            info!("No match was at given instance node id");
            debug!(
                "Could not find {:?} among matches {:?}, returning empty response",
                &crane_event.instance_node_id, &matches
            );
            return empty_response();
        }
        let object = object.unwrap();

        let mut found_barrel_at_crane_tuple = None;
        for m in matches {
            for match_tuple in &m.tuples {
                if match_tuple
                    .trg
                    .as_ref()
                    .unwrap()
                    .trg
                    .as_ref()
                    .unwrap()
                    .node_type
                    == "Crane"
                {
                    if found_barrel_at_crane_tuple.is_none() {
                        found_barrel_at_crane_tuple = Some(match_tuple);
                    } else if found_barrel_at_crane_tuple
                        .unwrap()
                        .trg
                        .as_ref()
                        .unwrap()
                        .from_timestamp
                        < match_tuple
                            .trg
                            .as_ref()
                            .unwrap()
                            .from_timestamp
                    {
                        found_barrel_at_crane_tuple = Some(match_tuple);
                    }
                }
            }
        }

        if found_barrel_at_crane_tuple.is_none() {
            info!("There was no barrel at the crane, returning empty response");
            debug!(
                "Could not find barrel at crane among matches {:?}, returning empty response",
                &matches
            );
            return empty_response();
        }

        let barrel_at_crane_tuple = found_barrel_at_crane_tuple.unwrap();
        let barrel_removed_from_crane = Delta {
            src: barrel_at_crane_tuple.trg.as_ref().unwrap().src.clone(),
            trg: barrel_at_crane_tuple.trg.as_ref().unwrap().trg.clone(),
            edge_type: barrel_at_crane_tuple
                .trg
                .as_ref()
                .unwrap()
                .edge_type
                .clone(),
            timestamp: timestamp,
            delta_type: DeltaType::Removal as i32,
        };
        let barrel_added_to_crane = Delta {
            src: barrel_at_crane_tuple.trg.as_ref().unwrap().src.clone(),
            trg: Some(as_instance_node(&object)),
            edge_type: barrel_at_crane_tuple
                .trg
                .as_ref()
                .unwrap()
                .edge_type
                .clone(),
            timestamp: timestamp + 1,
            delta_type: DeltaType::Addition as i32,
        };
        let deltas = vec![barrel_removed_from_crane, barrel_added_to_crane];
        debug!("Successfully processed drop event, returning deltas");
        Ok(Response::new(ApplicationResponse { deltas }))
    }
}
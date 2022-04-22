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


use crate::common::application_component::{ApplicationRequest, ApplicationResponse, Match, QueryEdge};
use crate::common::delta::{Delta, DeltaType};
use bincode::{config::Configuration, Decode, Encode};
use log::{debug, warn};
use tonic::{Response, Status};
use crate::common::{as_instance_node, empty_response};
use crate::detector::BarrelMaterialType;


#[derive(Decode, Encode, Debug)]
pub struct ConveyorEvent {
}

pub(crate) struct ConveyorApplication {
    config: Configuration,
}

impl ConveyorApplication {
    pub(crate) fn new() -> ConveyorApplication {
        ConveyorApplication {
            config: bincode::config::standard(),
        }
    }

    pub(crate) fn conveyor_function(
        &self,
        request: &ApplicationRequest,
    ) -> Result<Response<ApplicationResponse>, Status> {
        debug!("Conveyor request: {:?}", &request);
        match &request.event {
            Some(event) => {
                let (conveyor_event, _): (ConveyorEvent, usize) =
                    bincode::decode_from_slice(&event.payload, self.config)
                        .expect("Error decoding");
                let resp = self.process_conveyor_event(
                    event.timestamp,
                    conveyor_event,
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

    fn process_conveyor_event(&self, timestamp:u64, _conveyor_event:ConveyorEvent,
        matches: &Vec<Match>,
        query_graph: &Vec<QueryEdge>) -> Result<Response<ApplicationResponse>, Status> {
        assert_eq!(matches.len(), 1);
        let single_match = matches.get(0).unwrap();
        assert_eq!(single_match.tuples.len(), 2);
        let mut barrel_at_conveyor = None;
        let mut barrel_has_type = None;
        for t in &single_match.tuples {
            if t.src.as_ref().unwrap().edge_type == "At" {
                barrel_at_conveyor = t.trg.as_ref();
            } else if t.src.as_ref().unwrap().edge_type == "HasMaterialType" {
                    barrel_has_type = t.trg.as_ref();

            }
        }
        assert!(barrel_at_conveyor.is_some());
        assert!(barrel_has_type.is_some());

        let mut plastic_ramp = None;
        let mut metal_ramp = None;

        for qn in query_graph {
            if qn.trg.as_ref().unwrap().query_node_name == "m" {
                metal_ramp = qn.trg.as_ref();
            } else if qn.trg.as_ref().unwrap().query_node_name == "p" {
                plastic_ramp = qn.trg.as_ref();
            }
        }
        assert!(plastic_ramp.is_some());
        assert!(metal_ramp.is_some());

        let remove_barrel = Delta {
            src: barrel_at_conveyor.as_ref().unwrap().src.clone(),
            trg: barrel_at_conveyor.as_ref().unwrap().trg.clone(),
            edge_type: "At".to_string(),
            timestamp,
            delta_type: DeltaType::Removal as i32,
        };
        let (barrel_type, _): (BarrelMaterialType, usize)  = bincode::decode_from_slice(
            &barrel_has_type.unwrap().trg.as_ref().unwrap().value.as_ref().unwrap().b,
            self.config).expect("Decoding problem");
        let trg_node = match barrel_type {
            BarrelMaterialType::Metal => {metal_ramp.unwrap()}
            BarrelMaterialType::Plastic => {plastic_ramp.unwrap()}
        };
        let add_barrel = Delta {
            src : barrel_at_conveyor.as_ref().unwrap().src.clone(),
            trg : Some(as_instance_node(trg_node)),
            edge_type: "At".to_string(),
            timestamp: timestamp + 1,
            delta_type: DeltaType::Addition as i32,
        };

        Ok(Response::new(ApplicationResponse{ deltas: vec![remove_barrel, add_barrel] }))
    }
}
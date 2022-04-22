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
use crate::common::delta::{Delta, DeltaType, InstanceNode, NodeClass, OptionalBytes};

use bincode::{config::Configuration, Decode, Encode};
use log::{debug, warn};
use tonic::{Response, Status};
use crate::common::empty_response;

#[derive(Decode, Encode, Clone, Debug, PartialEq)]
pub enum BarrelMaterialType {
    Metal,
    Plastic
}

#[derive(Decode, Encode, Debug)]
pub struct DetectorEvent {
    pub barrel_material_type: BarrelMaterialType,
}

pub(crate) struct DetectorApplication {
    config: Configuration,
}

impl DetectorApplication {
    pub(crate) fn new() -> DetectorApplication {
        DetectorApplication {
            config: bincode::config::standard(),
        }
    }

    pub(crate) fn detector_function(
        &self,
        request: &ApplicationRequest,
    ) -> Result<Response<ApplicationResponse>, Status> {
        debug!("Detector request: {:?}", &request);
        match &request.event {
            Some(event) => {
                let (detector_event, _): (DetectorEvent, usize) =
                    bincode::decode_from_slice(&event.payload, self.config)
                        .expect("Error decoding");
                let resp = self.process_detector_event(
                    event.timestamp,
                    detector_event,
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

    fn process_detector_event(&self, timestamp:u64, detector_event:DetectorEvent, matches: &Vec<Match>,
        _query_graph: &Vec<QueryEdge>) -> Result<Response<ApplicationResponse>, Status>{
        assert_eq!(matches.len(), 1);
        let single_match = matches.get(0).unwrap();
        assert_eq!(single_match.tuples.len(), 1);
        let single_tuple = single_match.tuples.get(0).unwrap();
        let barrel = single_tuple.trg.as_ref().unwrap().src.as_ref().unwrap();
        let barrel_type = detector_event.barrel_material_type;

        let material_type_node = create_material_type_node(barrel.instance_node_id.clone(), barrel_type, self.config);

        let out_delta = Delta{
            src: single_tuple.trg.as_ref().unwrap().src.clone(),
            trg: Some(material_type_node),
            edge_type: "HasMaterialType".to_string(),
            timestamp:timestamp + 1,
            delta_type: DeltaType::Addition as i32,
        };
        Ok(Response::new(ApplicationResponse{deltas:vec![out_delta]}))
    }
}

pub fn create_material_type_node(barrel_node_id: String, barrel_material_type: BarrelMaterialType, config: Configuration) -> InstanceNode {
    let barrel_type_bytes = bincode::encode_to_vec(barrel_material_type, config).expect("Encoding error");
    InstanceNode{
            instance_node_id: barrel_node_id + "_barrel_type",
            node_type: "BarrelMaterialType".to_string(),
            node_class: NodeClass::Property as i32,
            value: Some(OptionalBytes{b:barrel_type_bytes}),
        }
}
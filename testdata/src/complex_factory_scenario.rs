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

use bincode::config::{Configuration, standard};
use std::collections::BTreeMap;

use log::{debug, info};
use mbei_core::event::{Event, Update};
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use uuid::Uuid;

use mbei_core::graph::{Delta, DeltaType, Node};
use mbei_core::query::Query;
use mbei_scenario_server::conveyor::ConveyorEvent;
use mbei_scenario_server::detector::{
    create_material_type_node, BarrelMaterialType, DetectorEvent,
};

use crate::factory_scenario_builder::ComplexFactoryScenario;
use crate::factory_scenario_common_updates::{
    create_drop_message, create_objects_surrounding_crane_map,
    create_pickdrop_belongs_to_crane_map, create_pickup_message, create_platform_message,
    create_ramp_message, create_stamp_assembly_of_map, create_stamp_message,
    create_topic_names_map, new_barrel,
};
use crate::message_creator::MessageCreator;
use crate::producer::{TopicNameAndUpdate};

pub struct ComplexFactoryScenarioSimulator {
    config: Configuration,
    current_timestamp: u64,
    pub state: BTreeMap<Node, Option<Node>>,
    at_candidates: Vec<Node>,
    topic_names_map: BTreeMap<Node, Vec<String>>,
    stamp_assembly_of_map: BTreeMap<Node, Node>,
    objects_surrounding_crane_map: BTreeMap<Node, Vec<Node>>,
    pickdrop_belongs_to_crane_map: BTreeMap<Node, Node>,
    conveyor_of_conveyor_event: BTreeMap<Node, Node>,
    plastic_ramp_of_conveyor: BTreeMap<Node, Node>,
    metal_ramp_of_conveyor: BTreeMap<Node, Node>,
    barrel_material_type: BTreeMap<Node, BarrelMaterialType>,
    detector_of_platform: BTreeMap<Node, Node>,
    generator: ChaCha8Rng,
    use_central: bool,
}

impl ComplexFactoryScenarioSimulator {
    pub fn new(
        factory_scenario: ComplexFactoryScenario,
        init_timestamp: u64,
        use_central: bool,
    ) -> ComplexFactoryScenarioSimulator {
        let mut state = BTreeMap::new();
        let mut at_candidates = factory_scenario.platforms.clone();
        at_candidates.append(&mut factory_scenario.stamp_assemblies.clone());
        at_candidates.append(&mut factory_scenario.crane_pickdrops.clone());
        at_candidates.append(&mut factory_scenario.conveyor_events.clone());
        at_candidates.append(&mut factory_scenario.detectors.clone());
        at_candidates.append(&mut factory_scenario.metal_ramps.clone());
        at_candidates.append(&mut factory_scenario.plastic_ramps.clone());

        //The state records if there is a barrel at the possible location
        for c in &at_candidates {
            state.insert(c.clone(), None);
        }
        //Dirty fix for no conveyor state, as is required for all objcets surrounding a crane.
        for c in &factory_scenario.conveyors {
            state.insert(c.clone(), None);
        }

        let all_queries = factory_scenario.all_queries();

        let topic_names_map = create_topic_names_map(&at_candidates, &all_queries);
        let stamp_assembly_of_map = create_stamp_assembly_of_map(&all_queries);
        let objects_surrounding_crane_map = create_objects_surrounding_crane_map(&all_queries);
        let pickdrop_belongs_to_crane_map = create_pickdrop_belongs_to_crane_map(&all_queries);
        let conveyor_of_conveyor_event = create_conveyor_of_conveyor_event(&all_queries);
        let plastic_ramp_of_conveyor = create_plastic_ramp_of_conveyor(&all_queries);
        let metal_ramp_of_conveyor = create_metal_ramp_of_conveyor(&all_queries);
        let detector_of_platform = create_detector_of_platform(&all_queries);
        let barrel_material_type = BTreeMap::new();

        // Adapted from: https://stackoverflow.com/questions/67502578/how-to-produce-reproducible-pseudo-random-numbers-based-on-a-seed-for-all-platfo
        let generator: ChaCha8Rng = rand_chacha::ChaCha8Rng::seed_from_u64(1234);

        ComplexFactoryScenarioSimulator {
            current_timestamp: init_timestamp,
            state,
            at_candidates,
            topic_names_map,
            stamp_assembly_of_map,
            objects_surrounding_crane_map,
            pickdrop_belongs_to_crane_map,
            conveyor_of_conveyor_event,
            plastic_ramp_of_conveyor,
            metal_ramp_of_conveyor,
            barrel_material_type,
            detector_of_platform,
            generator,
            use_central,
            config:standard()
        }
    }
}

impl MessageCreator for ComplexFactoryScenarioSimulator {
    fn create_one_new_message(
        &mut self,
        stopping: bool,
    ) -> (Vec<Delta>, Vec<TopicNameAndUpdate>) {
        let n_candidates = self.at_candidates.len();

        loop {
            let mut sent = false;
            let mut topic_names_and_updates: Vec<TopicNameAndUpdate> = vec![];
            let mut inferred_deltas: Vec<Delta> = vec![];
            let r = self.generator.next_u64() % (n_candidates as u64);
            let c = self.at_candidates.get(r as usize).unwrap().clone();
            debug!("Candidate: {:?}", &c.instance_node_name);
            let topic_names = self.topic_names_map.get(&c).unwrap().clone();
            //No new nodes if stopping
            if !stopping && c.node_type.as_ref().unwrap() == "Platform" {
                if self.state.get(&c).unwrap().is_none() {
                    let barrel = new_barrel();
                    topic_names_and_updates.append(&mut create_platform_message(
                        barrel.clone(),
                        c.clone(),
                        &mut self.state,
                        self.current_timestamp,
                        &mut inferred_deltas,
                        topic_names,
                        self.use_central,
                    ));
                    let detector = self.detector_of_platform.get(&c).unwrap();
                    let barrel_material_type;
                    if self.current_timestamp % 2 == 0 {
                        barrel_material_type = BarrelMaterialType::Metal;
                    } else {
                        barrel_material_type = BarrelMaterialType::Plastic;
                    }
                    self.barrel_material_type
                        .insert(barrel.clone(), barrel_material_type.clone());
                    let topic_name = self.topic_names_map.get(detector).unwrap().get(0).unwrap();
                    topic_names_and_updates.push(create_detector_message(
                        barrel,
                        barrel_material_type,
                        detector.clone(),
                        self.current_timestamp,
                        &mut inferred_deltas,
                        topic_name.clone(),
                        self.config,
                    ));
                    sent = true;
                }
            } else if c.node_type.as_ref().unwrap() == "PickDrop" {
                let crane = self.pickdrop_belongs_to_crane_map.get(&c).unwrap();
                let surrounding = &self.objects_surrounding_crane_map.get(&crane).unwrap();
                let s = self.generator.next_u64() % (surrounding.len() as u64);
                let o = surrounding.get(s as usize).unwrap().clone();
                let topic_names = &self.topic_names_map.get(&c).unwrap();
                let topic_name = topic_names.get(0).unwrap().clone();
                if self.state.get(&c).unwrap().is_none() {
                    //Picking up only from other places than the ramp (final destination)
                    if !stopping || (o.node_type.as_ref().unwrap() != "Ramp") {
                        if let Some(node) = self.state.get(&o).unwrap() {
                            topic_names_and_updates.push(create_pickup_message(
                                node.clone(),
                                crane.clone(),
                                c.clone(),
                                o.clone(),
                                &mut self.state,
                                self.current_timestamp,
                                &mut inferred_deltas,
                                topic_name,
                                self.config,
                            ));
                            sent = true;
                        }
                    }
                } else if let Some(node) = self.state.get(&c).unwrap() {
                    //Dropping off only at conveyor if we are stopping
                    if !stopping || (o.node_type.as_ref().unwrap() == "Conveyor") {
                        if self.state.get(&o).unwrap().is_none() {
                            topic_names_and_updates.push(create_drop_message(
                                node.clone(),
                                crane.clone(),
                                c.clone(),
                                o.clone(),
                                &mut self.state,
                                self.current_timestamp,
                                &mut inferred_deltas,
                                topic_name,
                                self.config,
                            ));
                            sent = true;
                        }
                    }
                }
            } else if !stopping && c.node_type.as_ref().unwrap() == "Stamp" {
                let stamp_assembly = self.stamp_assembly_of_map.get(&c).unwrap();
                if self.state.get(stamp_assembly).unwrap().is_some() {
                    topic_names_and_updates.push(create_stamp_message(
                        c.clone(),
                        stamp_assembly.clone(),
                        &self.state,
                        &self.topic_names_map,
                        self.current_timestamp,
                        &mut inferred_deltas,
                        self.config,
                    ));
                    sent = true;
                }
            } else if c.node_type.as_ref().unwrap() == "ConveyorEvent" {
                let conveyor = self.conveyor_of_conveyor_event.get(&c).unwrap();
                let plastic_ramp = self.plastic_ramp_of_conveyor.get(&conveyor).unwrap();
                let metal_ramp = self.metal_ramp_of_conveyor.get(&conveyor).unwrap();
                if let Some(barrel) = self.state.get(&conveyor).unwrap() {
                    let barrel_material_type = self.barrel_material_type.get(&barrel).unwrap();
                    let use_ramp;
                    if barrel_material_type == &BarrelMaterialType::Metal {
                        use_ramp = metal_ramp;
                    } else {
                        use_ramp = plastic_ramp;
                    }
                    if self.state.get(use_ramp).unwrap().is_none() {
                        let topic_name = self
                            .topic_names_map
                            .get(&c)
                            .unwrap()
                            .get(0)
                            .unwrap()
                            .clone();
                        topic_names_and_updates.push(produce_conveyor_message(
                            barrel.clone(),
                            conveyor.clone(),
                            c.clone(),
                            use_ramp.clone(),
                            &mut self.state,
                            self.current_timestamp,
                            &mut inferred_deltas,
                            topic_name,
                            self.config,
                        ));
                        sent = true;
                    }
                }
            } else if c.node_type.as_ref().unwrap() == "Ramp" {
                if let Some(barrel) = self.state.get(&c).unwrap() {
                    self.barrel_material_type.remove(barrel);
                    let topic_names = self.topic_names_map.get(&c).unwrap().clone();
                    topic_names_and_updates.append(&mut create_ramp_message(
                        barrel.clone(),
                        c.clone(),
                        &mut self.state,
                        self.current_timestamp,
                        &mut inferred_deltas,
                        topic_names,
                        self.use_central,
                    ));
                    sent = true;
                }
            }
            if sent {
                self.current_timestamp += 3;
                break (inferred_deltas, topic_names_and_updates);
            }
        }
    }

    fn is_finished(&self) -> bool {
        !self.state.values().any(|x| x.is_some())
    }

    fn get_current_timestamp(&self) -> u64 {
        self.current_timestamp
    }
}

fn create_detector_of_platform(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut detector_of_platform = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.src.node_type.as_ref().unwrap() == "Platform" && e.edge_type == "HasEvent" {
                detector_of_platform.insert(src_clone, trg_clone);
            }
        }
    }
    detector_of_platform
}

fn create_metal_ramp_of_conveyor(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut metal_ramp_of_conveyor = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.edge_type == "HasMetalRamp" {
                metal_ramp_of_conveyor.insert(src_clone, trg_clone);
            }
        }
    }
    metal_ramp_of_conveyor
}

fn create_plastic_ramp_of_conveyor(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut plastic_ramp_of_conveyor = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.edge_type == "HasPlasticRamp" {
                plastic_ramp_of_conveyor.insert(src_clone, trg_clone);
            }
        }
    }
    plastic_ramp_of_conveyor
}

fn create_conveyor_of_conveyor_event(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut conveyor_of_conveyor_event = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.src.node_type.as_ref().unwrap() == "Conveyor"
                && e.trg.node_type.as_ref().unwrap() == "ConveyorEvent"
            {
                conveyor_of_conveyor_event.insert(trg_clone, src_clone);
            }
        }
    }
    conveyor_of_conveyor_event
}

fn produce_conveyor_message(
    barrel: Node,
    conveyor: Node,
    conveyor_event: Node,
    use_ramp: Node,
    state: &mut BTreeMap<Node, Option<Node>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_name: String,
    config: Configuration,
) -> TopicNameAndUpdate {
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: conveyor.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp,
        delta_type: DeltaType::Removal,
    });
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: use_ramp.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp + 1,
        delta_type: DeltaType::Addition,
    });

    let cloned_barrel = Some(barrel.clone());
    state.insert(conveyor.clone(), None);
    state.insert(use_ramp, cloned_barrel);

    create_conveyor_event(
        conveyor_event.clone(),
        current_timestamp,
        topic_name,
        config,
    )
}

fn create_conveyor_event(
    conveyor_event_node: Node,
    timestamp: u64,
    topic: String,
    config: Configuration,
) -> TopicNameAndUpdate {
    info!(
        "Conveyor event at {}: {}",
        &timestamp,
        &conveyor_event_node.instance_node_name.as_ref().unwrap(),
    );
    let conveyor_event = ConveyorEvent {};
    let event = Event {
        event_id: Uuid::new_v4().to_hyphenated().to_string(),
        timestamp,
        node_id: conveyor_event_node
            .instance_node_name
            .as_ref()
            .unwrap()
            .clone(),
        payload: bincode::encode_to_vec(&conveyor_event, config).expect("Encoding error"),
    };
    TopicNameAndUpdate::new(topic, Update::Event(event))
}

fn create_detector_message(
    barrel: Node,
    barrel_material_type: BarrelMaterialType,
    detector: Node,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_name: String,
    config: Configuration,
) -> TopicNameAndUpdate {
    //This is quite a dirty fix..
    let trg_node_wrong_classtype = create_material_type_node(
        barrel.instance_node_name.as_ref().unwrap().clone(),
        barrel_material_type.clone(),
        config,
    );
    let trg_node = Node::property_instance_node(
        &trg_node_wrong_classtype.instance_node_id,
        &trg_node_wrong_classtype.node_type,
        trg_node_wrong_classtype.value.unwrap().b,
    );

    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: trg_node,
        edge_type: "HasMaterialType".to_string(),
        timestamp: current_timestamp + 1,
        delta_type: DeltaType::Addition,
    });
    create_detector_event(
        detector,
        barrel_material_type,
        current_timestamp,
        topic_name,
        config,
    )
}

fn create_detector_event(
    detector: Node,
    barrel_material_type: BarrelMaterialType,
    timestamp: u64,
    topic: String,
    config: Configuration,
) -> TopicNameAndUpdate {
    info!(
        "Detector event at {}: {}",
        &timestamp,
        &detector.instance_node_name.as_ref().unwrap()
    );
    let detector_event = DetectorEvent {
        barrel_material_type,
    };
    let payload = bincode::encode_to_vec(&detector_event, config).expect("Encoding error");
    let event = Event {
        event_id: Uuid::new_v4().to_hyphenated().to_string(),
        timestamp,
        node_id: detector.instance_node_name.as_ref().unwrap().clone(),
        payload: payload.clone(),
    };
    TopicNameAndUpdate::new(topic, Update::Event(event))
}

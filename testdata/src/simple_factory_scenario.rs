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

use std::collections::BTreeMap;
use bincode::config::{Configuration, standard};

use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use mbei_core::graph::{Delta, Node};

use crate::factory_scenario_builder::SimpleFactoryScenario;
use crate::factory_scenario_common_updates::{
    create_objects_surrounding_crane_map, create_pickdrop_belongs_to_crane_map,
    create_stamp_assembly_of_map, create_topic_names_map, new_barrel, create_drop_message, create_pickup_message,
    create_platform_message, create_ramp_message, create_stamp_message,
};
use crate::message_creator::MessageCreator;
use crate::producer::{TopicNameAndUpdate};

pub struct SimpleFactoryScenarioSimulator {
    config: Configuration,
    current_timestamp: u64,
    pub state: BTreeMap<Node, Option<Node>>,
    at_candidates: Vec<Node>,
    topic_names_map: BTreeMap<Node, Vec<String>>,
    stamp_assembly_of_map: BTreeMap<Node, Node>,
    objects_surrounding_crane_map: BTreeMap<Node, Vec<Node>>,
    pickdrop_belongs_to_crane_map: BTreeMap<Node, Node>,
    generator: ChaCha8Rng,
    use_central: bool
}

impl SimpleFactoryScenarioSimulator {
    pub fn new(
        factory_scenario: SimpleFactoryScenario,
        init_timestamp: u64,
        use_central: bool
    ) -> SimpleFactoryScenarioSimulator {
        let mut state = BTreeMap::new();
        let mut at_candidates = factory_scenario.platforms.clone();
        at_candidates.append(&mut factory_scenario.stamp_assemblies.clone());
        at_candidates.append(&mut factory_scenario.crane_pickdrops.clone());
        at_candidates.append(&mut factory_scenario.ramps.clone());

        //The state records if there is a barrel at the possible location
        for c in &at_candidates {
            state.insert(c.clone(), None);
        }

        let topic_names_map = create_topic_names_map(&at_candidates, &factory_scenario.queries);
        let stamp_assembly_of_map = create_stamp_assembly_of_map(&factory_scenario.queries);
        let objects_surrounding_crane_map =
            create_objects_surrounding_crane_map(&factory_scenario.queries);
        let pickdrop_belongs_to_crane_map =
            create_pickdrop_belongs_to_crane_map(&factory_scenario.queries);

        // Adapted from: https://stackoverflow.com/questions/67502578/how-to-produce-reproducible-pseudo-random-numbers-based-on-a-seed-for-all-platfo
        let generator: ChaCha8Rng = rand_chacha::ChaCha8Rng::seed_from_u64(1234);

        SimpleFactoryScenarioSimulator {
            current_timestamp: init_timestamp,
            state,
            at_candidates,
            topic_names_map,
            stamp_assembly_of_map,
            objects_surrounding_crane_map,
            pickdrop_belongs_to_crane_map,
            generator,
            use_central,
            config:standard()
        }
    }
}

impl MessageCreator for SimpleFactoryScenarioSimulator {
    fn create_one_new_message(
        &mut self,
        stopping: bool,
    ) -> (Vec<Delta>, Vec<TopicNameAndUpdate>) {
        let n_candidates = self.at_candidates.len();

        loop {
            let mut sent = false;
            let mut topics_and_updates: Vec<TopicNameAndUpdate> = vec![];
            let mut inferred_deltas: Vec<Delta> = vec![];
            let r = self.generator.next_u64() % (n_candidates as u64);
            let c = self.at_candidates.get(r as usize).unwrap().clone();
            let topic_names = self.topic_names_map.get(&c).unwrap().clone();
            //No new nodes if stopping
            if !stopping && c.node_type.as_ref().unwrap() == "Platform" {
                if self.state.get(&c).unwrap().is_none() {
                    let barrel = new_barrel();
                    topics_and_updates.append(
                        &mut create_platform_message(
                            barrel,
                            c.clone(),
                            &mut self.state,
                            self.current_timestamp,
                            &mut inferred_deltas,
                            topic_names,
                            self.use_central
                        )
                    );
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
                            topics_and_updates.push(
                                create_pickup_message(
                                    node.clone(),
                                    crane.clone(),
                                    c.clone(),
                                    o.clone(),
                                    &mut self.state,
                                    self.current_timestamp,
                                    &mut inferred_deltas,
                                    topic_name,
                                    self.config
                                )
                            );
                            sent = true;
                        }
                    }
                } else if let Some(node) = self.state.get(&c).unwrap() {
                    //Dropping off only at ramp if we are stopping
                    if !stopping || (o.node_type.as_ref().unwrap() == "Ramp") {
                        if self.state.get(&o).unwrap().is_none() {
                            topics_and_updates.push(
                                create_drop_message(
                                    node.clone(),
                                    crane.clone(),
                                    c.clone(),
                                    o.clone(),
                                    &mut self.state,
                                    self.current_timestamp,
                                    &mut inferred_deltas,
                                    topic_name,
                                    self.config
                                )
                            );
                            sent = true;
                        }
                    }
                }
            } else if !stopping && c.node_type.as_ref().unwrap() == "Stamp" {
                let stamp_assembly = self.stamp_assembly_of_map.get(&c).unwrap();
                if self.state.get(stamp_assembly).unwrap().is_some() {
                    topics_and_updates.push(
                        create_stamp_message(
                            c.clone(),
                            stamp_assembly.clone(),
                            &self.state,
                            &self.topic_names_map,
                            self.current_timestamp,
                            &mut inferred_deltas,
                            self.config
                        )
                    );
                    sent = true;
                }
            } else if c.node_type.as_ref().unwrap() == "Ramp" {
                if let Some(barrel) = self.state.get(&c).unwrap() {
                    let topic_names = self.topic_names_map.get(&c).unwrap().clone();
                    topics_and_updates.append(
                        &mut create_ramp_message(
                            barrel.clone(),
                            c.clone(),
                            &mut self.state,
                            self.current_timestamp,
                            &mut inferred_deltas,
                            topic_names,
                            self.use_central
                        )
                    );
                    sent = true;
                }
            }
            if sent {
                self.current_timestamp += 3;
                break (inferred_deltas, topics_and_updates);
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

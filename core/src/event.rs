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

use std::collections::BTreeSet;
use bincode::{Decode, Encode};
use bincode::config::Configuration;
use seahash::hash;
use serde::{Serialize, Deserialize};

use crate::graph::Delta;

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub struct Event {
    pub event_id: String,
    pub timestamp: u64,
    pub node_id: String,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub struct Deltas {
    pub deltas_id: String,
    pub origin_id: String,
    pub origin_timestamp: u64,
    pub deltas: BTreeSet<Delta>,
}

impl Deltas {
    pub fn stable_hash(&self, c: Configuration) -> u64 {
        hash(
            bincode::encode_to_vec(self, c)
                .expect("Encodable")
                .as_slice(),
        )
    }
}

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub struct Retractions {
    pub retraction_id: String,
    pub timestamp: u64,
    pub deltas_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Encode, Decode, Clone, Debug, PartialEq)]
pub enum Update {
    Stop,
    Event(Event),
    Deltas(Deltas),
    Retractions(Retractions),
}

impl Update {
    pub fn timestamp(&self) -> u64 {
        match self {
            Update::Event(e) => {e.timestamp}
            Update::Deltas(ds) => {ds.origin_timestamp }
            Update::Retractions(rt) => {rt.timestamp}
            _ => {panic!("Not defined")}
        }
    }

    pub fn event_id(&self) -> &str {
        match self {
            Update::Event(e) => { &e.event_id }
            Update::Deltas(ds) => { &ds.origin_id }
            _ => {panic!("Not defined")}
        }
    }
}
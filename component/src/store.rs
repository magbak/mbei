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

use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::time::Instant;

use log::{debug, info};

use mbei_core::event::{Deltas, Event};
use mbei_core::graph::{Delta, DeltaType, Edge, NodeClass};

pub struct Store {
    deltas_by_deltas_id: BTreeMap<String, Deltas>,
    deltas_and_deltas_id_by_edge: BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>>,
    retracted_deltas_ids: BTreeSet<String>,
    event_ids_by_timestamp: BTreeMap<u64, BTreeSet<String>>,
    events_by_event_id: BTreeMap<String, Event>,
    edge_grid: [BTreeMap<u64, BTreeSet<Edge>>;4],
    event_match_hash_and_output_hash: BTreeMap<String, BTreeMap<u64, Option<u64>>>,
    matches_hashes_deltas_ids: BTreeMap<String, Vec<TopicNameAndDeltasId>>,
    edges_by_node: BTreeMap<String, BTreeSet<Edge>>,
    pub(crate) open_edges: BTreeSet<Edge>,
    watermark: u64,
    levels: [u64;4]
}

impl Store {
    pub fn new() -> Store {
        Store {
            deltas_by_deltas_id: Default::default(),
            deltas_and_deltas_id_by_edge: Default::default(),
            retracted_deltas_ids: Default::default(),
            event_ids_by_timestamp: Default::default(),
            events_by_event_id: Default::default(),
            edge_grid: Default::default(),
            event_match_hash_and_output_hash: Default::default(),
            matches_hashes_deltas_ids: Default::default(),
            edges_by_node: Default::default(),
            open_edges: BTreeSet::new(),
            watermark: 0,
            levels: [10,100,1000, u64::MAX/2] //Avoids overflow
        }
    }

    pub fn add_new_event(&mut self, event: &Event) {
        self.event_ids_by_timestamp.entry(event.timestamp).or_insert(BTreeSet::new()).insert(event.event_id.clone());
        self.events_by_event_id.insert(event.event_id.clone(), event.clone());
    }

    pub fn add_new_match_updates_binding(
        &mut self,
        event_id: &str,
        match_hash: &u64,
        topic_names_deltas_ids: Vec<TopicNameAndDeltasId>,
    ) {
        let match_event_string = create_match_event_string(event_id, match_hash);
        self.matches_hashes_deltas_ids.insert(match_event_string, topic_names_deltas_ids);
    }

    pub(crate) fn update_matches(&mut self, event_id: &str, updated_matches_hashes: BTreeMap<u64, Option<u64>>) {
        self.event_match_hash_and_output_hash.insert(event_id.to_string(), updated_matches_hashes);
    }

    pub fn pop_deltas_ids_for_event_id_and_match_hash(
        &mut self,
        event_id: &str,
        match_hash: &u64,
    ) -> Option<Vec<TopicNameAndDeltasId>> {
        let match_update_string = create_match_event_string(event_id, match_hash);
        self.matches_hashes_deltas_ids.remove(&match_update_string)
    }

    pub fn get_event_output_hash_by_match_hash(&self, event_id: &str) -> BTreeMap<u64, Option<u64>> {
        match self.event_match_hash_and_output_hash.get(event_id) {
            None => {BTreeMap::new()}
            Some(t) => {t.clone()}
        }

    }

    pub fn update_edges(&mut self, updated_edges: &Vec<Edge>, existing_edges: &Vec<Edge>) {
        let now = Instant::now();
        let newset = BTreeSet::from_iter(updated_edges);
        let oldset = BTreeSet::from_iter(existing_edges);

        for e in &newset {
            if !oldset.contains(e) {
                self.add_edge((*e).clone());
            }
        }
        for e in oldset {
            if !newset.contains(e) {
                self.delete_edge(e);
            }
        }
        info!("Update edges processing took {} μs", now.elapsed().as_micros());
    }

    pub fn delete_edge(&mut self, e: &Edge) {
        debug!("Deleting edge {:?}", e);
        if index_edge_by_node(e) {
            self.delete_node_indexed_edge(e);
        }
        if e.to_timestamp.is_none() {
            debug!("Removed from open");
            self.open_edges.remove(e);
        } else {
            debug!("Removed from closed");
            let (index, level) = self.find_edge_index_and_level(e);
            let from_bin = e.from_timestamp.unwrap() / (level);
            let to_bin = e.to_timestamp.unwrap() / (level);

            self.delete_edge_from_bin(e, index, &from_bin);
            if to_bin != from_bin {
                self.delete_edge_from_bin(e, index, &to_bin);
            }
        }
    }

    fn find_edge_index_and_level(&self, e: &Edge) -> (usize, u64) {
        let duration = e.to_timestamp.unwrap() - e.from_timestamp.unwrap();
        for (i, l) in self.levels.iter().enumerate() {
            if 2*l >= duration {
                return (i, l.clone());
            }
        }
        panic!("Should never happen");
    }

    fn delete_edge_from_bin(&mut self, e: &Edge, bin_index: usize, bin: &u64) {
        if let Some(edges) = self.edge_grid[bin_index].get_mut(bin) {
            edges.remove(e);
        }
    }

    pub fn delete_node_indexed_edge(&mut self, e: &Edge) {
        let keys = index_edge_by_keys(e);
        for k in keys {
            if let Some(edges) = self.edges_by_node.get_mut(&k) {
                edges.remove(e);
            }
        }
    }

    pub fn get_node_indexed_edges_at_timestamp(
        &self,
        instance_node_ids: Vec<String>,
        timestamp: u64,
    ) -> Vec<Edge> {
        let mut visited_instance_node_ids = BTreeSet::new();
        let mut current_instance_node_ids = instance_node_ids;
        let mut return_edges = BTreeSet::new();

        while !current_instance_node_ids.is_empty() {
            let mut new_instance_node_ids = BTreeSet::new();
            for i in &current_instance_node_ids {
                visited_instance_node_ids.insert(i.clone());
            }
            let mut edges = BTreeSet::new();
            for s in &current_instance_node_ids {
                if let Some(new_edges) = self.edges_by_node.get(s) {
                    for e in new_edges {
                        edges.insert(e);
                    }
                }
            }

            for e in edges {
                if e.from_timestamp.unwrap() <= timestamp
                    && (e.to_timestamp.is_none() || e.to_timestamp.unwrap() >= timestamp)
                {
                    if e.src.node_class == NodeClass::Material {
                        if !visited_instance_node_ids
                            .contains(e.src.instance_node_name.as_ref().unwrap())
                        {
                            new_instance_node_ids
                                .insert(e.src.instance_node_name.as_ref().unwrap().clone());
                        }
                    }
                    if e.trg.node_class == NodeClass::Material {
                        if !visited_instance_node_ids
                            .contains(e.src.instance_node_name.as_ref().unwrap())
                        {
                            new_instance_node_ids
                                .insert(e.src.instance_node_name.as_ref().unwrap().clone());
                        }
                    }
                    return_edges.insert(e.clone());
                }
            }
            current_instance_node_ids = new_instance_node_ids.into_iter().collect();
        }
        return_edges.into_iter().collect()
    }

    pub fn add_edge(&mut self, e: Edge) {
        if index_edge_by_node(&e) {
            self.add_node_indexed_edge(e);
        } else if e.to_timestamp.is_none() {
            self.open_edges.insert(e);
        } else {
            let (index, level) = self.find_edge_index_and_level(&e);
            let from_bin = e.from_timestamp.unwrap() / (level);
            let to_bin = e.to_timestamp.unwrap() / (level);

            self.add_edge_to_bin(e.clone(), index, from_bin);
            if to_bin != from_bin {
                self.add_edge_to_bin(e, index, to_bin);
            }
        }
    }

    fn add_edge_to_bin(&mut self, edge: Edge, bin_index:usize, bin:u64) {
        self.edge_grid[bin_index].entry(bin).or_insert(BTreeSet::new()).insert(edge);
    }

    fn add_node_indexed_edge(&mut self, edge: Edge) {
        let keys = index_edge_by_keys(&edge);
        for k in keys {
            self.edges_by_node.entry(k).or_insert(BTreeSet::new()).insert(edge.clone());
        }
    }

    pub fn get_edges_at_timestamp(&self, timestamp: u64) -> Vec<Edge> {
        let now = Instant::now();
        let open_edges = self.get_open_edges_at_timestamp(timestamp);
        let mut all_edges = open_edges;

        if timestamp <= self.watermark {
            let mut closed_edges = self.get_closed_edges_at_timestamp(timestamp);
            all_edges.append(&mut closed_edges);
        }
        let mut materials_identifiers = vec![];
        for e in &all_edges {
            if e.src.node_class == NodeClass::Material {
                materials_identifiers.push(e.src.instance_node_name.as_ref().unwrap().clone());
            }
            if e.trg.node_class == NodeClass::Material {
                materials_identifiers.push(e.trg.instance_node_name.as_ref().unwrap().clone());
            }
        }
        if materials_identifiers.len() > 0 {
            let mut materials_indexed_edges =
                self.get_node_indexed_edges_at_timestamp(materials_identifiers, timestamp);
            all_edges.append(&mut materials_indexed_edges);
        }
        info!("Get edges at timestamp took {} μs", now.elapsed().as_micros());

        all_edges

    }

    pub fn get_closed_edges_at_timestamp(&self, timestamp: u64) -> Vec<Edge> {
        let mut unfiltered_edges = BTreeSet::new();

        for (level_index, level) in self.levels.iter().enumerate() {
            let timestamp_bin = timestamp / (level);

            let edges_opt = self.edge_grid[level_index].get(&timestamp_bin);
            if let Some(edges) = edges_opt {
                for e in edges {
                    unfiltered_edges.insert(e);
                }
            }
        }

        unfiltered_edges
        .into_iter()
        .filter(|e| {
            e.from_timestamp.unwrap() <= timestamp && timestamp <= e.to_timestamp.unwrap()
        }).map(|e| e.clone())
        .collect()
    }

    pub fn get_open_edges_at_timestamp(&self, timestamp: u64) -> Vec<Edge> {
        self.open_edges
            .iter()
            .map(|e| e.clone())
            .filter(|e| e.from_timestamp.unwrap() <= timestamp)
            .collect()
    }

    pub fn add_retractions(&mut self, retractions: &Vec<String>) {
        for update_id in retractions {
            self.retracted_deltas_ids.insert(update_id.clone());
        }
    }

    pub fn get_event_ids_in_interval(&self, from: u64, to: Option<u64>) -> Vec<String> {
        //MUST be be_bytes in order for lexicographic iteration to work
        let iter = match &to {
            None => {self.event_ids_by_timestamp.range(from..)}
            Some(to_actual) => {self.event_ids_by_timestamp.range(RangeInclusive::new(&from, to_actual))}
        };
        let mut return_event_ids = vec![];
        for (_timestamp, event_ids) in iter {
            for e in event_ids {
                return_event_ids.push(e.clone());
            }
        }
        return_event_ids
    }

    pub fn is_update_rectracted(&self, event_or_deltas_id: &str) -> bool {
        self.retracted_deltas_ids.contains(event_or_deltas_id)
    }

    pub fn add_deltas_and_get_updated_deltas_by_edge(
        &mut self,
        deltas: &Deltas,
    ) -> BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>> {
        self.add_deltas_to_deltas_by_deltas_id(deltas);
        self.update_watermark(deltas);
        self.add_deltas_to_deltas_by_edge_and_get_updated(deltas)
    }

    fn update_watermark(&mut self, deltas: &Deltas) {
        let removals = deltas
            .deltas
            .iter()
            .filter(|d| d.delta_type == DeltaType::Removal);
        let watermark_candidate_opt = removals.max_by_key(|d| d.timestamp);
        if let Some(watermark_candidate) = watermark_candidate_opt {
            self.watermark = max(watermark_candidate.timestamp, self.watermark);
        }
    }

    fn add_deltas_to_deltas_by_edge_and_get_updated(
        &mut self,
        deltas: &Deltas,
    ) -> BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>> {
        let mut deltas_by_edge = BTreeMap::new();
        let mut edge;
        for d in &deltas.deltas {
            edge = d.to_edge();
            let delta_and_deltas_id = DeltaAndDeltasId {
                delta: d.clone(),
                deltas_id: deltas.deltas_id.to_string(),
            };
            if !deltas_by_edge.contains_key(&edge) {
                deltas_by_edge.insert(edge, BTreeSet::from([delta_and_deltas_id]));
            } else {
                deltas_by_edge
                    .get_mut(&edge)
                    .unwrap()
                    .insert(delta_and_deltas_id);
            }
        }
        for (e, ds) in &mut deltas_by_edge {
            if !self.deltas_and_deltas_id_by_edge.contains_key(e) {
                self.deltas_and_deltas_id_by_edge.insert(e.clone(), (*ds).clone());
            } else {
                let existing_mut = self.deltas_and_deltas_id_by_edge.get_mut(e).unwrap();
                let mut existing_copy = existing_mut.clone();
                let mut new_copy = (*ds).clone();
                existing_mut.append(&mut new_copy);
                ds.append(&mut existing_copy);
            }
        }
        deltas_by_edge
    }

    fn add_deltas_to_deltas_by_deltas_id(&mut self, deltas: &Deltas) {
        self.deltas_by_deltas_id.insert(deltas.deltas_id.clone(), deltas.clone());
    }

    pub fn remove_deltas_by_edges_and_get_updated(
        &mut self,
        remove_deltas_by_edge: BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>>,
    ) -> BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>> {
        let mut deltas_and_deltas_id_by_edge = BTreeMap::new();
        for (e, remove) in remove_deltas_by_edge {
            if let Some(ds) = self.deltas_and_deltas_id_by_edge.get_mut(&e) {
                for r in remove {
                    ds.remove(&r);
                }
                deltas_and_deltas_id_by_edge.insert(e, (*ds).clone());
            }
            else {
                deltas_and_deltas_id_by_edge.insert(e, BTreeSet::new());
            }
        }
        deltas_and_deltas_id_by_edge
    }

    pub fn get_deltas_by_edge_vec(
        &self,
        edges: Vec<&Edge>,
    ) -> BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>> {
        let mut deltas_and_deltas_id_by_edge = BTreeMap::new();
        for e in edges {
            match self.deltas_and_deltas_id_by_edge.get(e) {
                None => {deltas_and_deltas_id_by_edge.insert(e.clone(), BTreeSet::new())}
                Some(ds) => {
                    deltas_and_deltas_id_by_edge.insert(e.clone(), (*ds).clone())}
            };
        }
        deltas_and_deltas_id_by_edge
    }

    pub fn pop_deltas_by_deltas_ids(&mut self, deltas_ids: &Vec<String>) -> Vec<DeltaAndDeltasId> {
        let deltas = self.get_deltas_by_deltas_ids(deltas_ids);
        self.drop_deltas_ids(deltas_ids);
        deltas
    }

    pub fn drop_deltas_ids(&mut self, update_ids: &Vec<String>) {
        for update_id in update_ids {
            self.deltas_by_deltas_id.remove(update_id);
        }
    }

    pub fn get_deltas_by_deltas_ids(&self, deltas_ids: &Vec<String>) -> Vec<DeltaAndDeltasId> {
        //TODO: Use multi_get_cf with tuples: (W,K)
        let mut return_deltas = vec![];
        for deltas_id in deltas_ids {
            if let Some(deltas) = self.deltas_by_deltas_id.get(deltas_id) {
                let mut new_deltas_and_deltas_ids = deltas.deltas
                    .iter()
                    .map(|d| DeltaAndDeltasId {
                        delta: d.clone(),
                        deltas_id: deltas_id.clone(),
                    })
                    .collect();
                return_deltas.append(&mut new_deltas_and_deltas_ids);
            }
        }
        return_deltas
    }

    pub fn get_event_by_event_id(&self, event_id: &String) -> Option<&Event> {
        self.events_by_event_id.get(event_id)
    }

    pub(crate) fn replace_old_match_with_equivalent_new_match(&mut self, event_id: &str, old_match_hash: &u64, new_match_hash: &u64) {
        let topic_names_and_deltas_ids_opt = self.pop_deltas_ids_for_event_id_and_match_hash(event_id, old_match_hash);
        assert!(topic_names_and_deltas_ids_opt.is_some());
        self.add_new_match_updates_binding(event_id, new_match_hash, topic_names_and_deltas_ids_opt.unwrap());
    }
}

fn create_match_event_string(event_id:&str, match_hash: &u64) -> String {
    let mut match_event_string = event_id.to_string();
    match_event_string += &*match_hash.to_string();
    match_event_string
}

pub struct TopicNameAndDeltasId {
    pub(crate) topic_name: String,
    pub(crate) deltas_id: String,
}

//We need this structure to ensure that retractions only retract deltas belonging to the deltas-update to be rectracted.
//When two different deltas-updates contains the same Delta, we need to represent that.
#[derive(Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct DeltaAndDeltasId {
    pub(crate) deltas_id: String,
    pub(crate) delta: Delta,
}

fn index_edge_by_node(e: &Edge) -> bool {
    e.src.node_class != NodeClass::Object && e.trg.node_class != NodeClass::Object
}

fn index_edge_by_keys(e: &Edge) -> Vec<String> {
    let mut keys = vec![];
    if e.src.node_class == NodeClass::Material {
        keys.push(e.src.instance_node_name.as_ref().unwrap().clone());
    }
    if e.trg.node_class == NodeClass::Material {
        keys.push(e.trg.instance_node_name.as_ref().unwrap().clone());
    }
    keys
}

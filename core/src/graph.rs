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

use bincode::{config::Configuration, Decode, Encode};
use rusqlite::types::{FromSql, FromSqlResult, ValueRef};
use seahash::hash;
use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Serialize, Deserialize, Decode, Encode, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Display)]
pub enum DeltaType {
    Addition = 0,
    Removal = 1,
}

impl FromSql for DeltaType {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        Ok(DeltaType::try_from(value.as_str().unwrap()).expect("Could not convert"))
    }
}

impl TryFrom<i32> for DeltaType {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            x if x == DeltaType::Addition as i32 => Ok(DeltaType::Addition),
            x if x == DeltaType::Removal as i32 => Ok(DeltaType::Removal),
            _ => Err(()),
        }
    }
}

impl TryFrom<&str> for DeltaType {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            x if x == "Addition" => Ok(DeltaType::Addition),
            x if x == "Removal" => Ok(DeltaType::Removal),
            _ => Err(()),
        }
    }
}

#[derive(
    Decode,
    Encode,
    Serialize,
    Deserialize,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Clone,
    Debug,
    Display,
)]
pub enum NodeClass {
    Object = 0,
    Event = 1,
    Material = 2,
    Property = 3,
}

impl FromSql for NodeClass {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        Ok(NodeClass::try_from(value.as_str().unwrap()).expect("Could not convert"))
    }
}

impl TryFrom<i32> for NodeClass {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            x if x == NodeClass::Object as i32 => Ok(NodeClass::Object),
            x if x == NodeClass::Event as i32 => Ok(NodeClass::Event),
            x if x == NodeClass::Material as i32 => Ok(NodeClass::Material),
            x if x == NodeClass::Property as i32 => Ok(NodeClass::Property),
            _ => Err(()),
        }
    }
}

impl TryFrom<&str> for NodeClass {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            x if x == "Object" => Ok(NodeClass::Object),
            x if x == "Event" => Ok(NodeClass::Event),
            x if x == "Material" => Ok(NodeClass::Material),
            x if x == "Property" => Ok(NodeClass::Property),
            _ => Err(()),
        }
    }
}

#[derive(Serialize, Deserialize, Decode, Encode, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Delta {
    pub src: Node,
    pub trg: Node,
    pub edge_type: String,
    pub timestamp: u64,
    pub delta_type: DeltaType,
}

impl Delta {
    pub fn to_edge(&self) -> Edge {
        Edge {
            src: self.src.clone(),
            trg: self.trg.clone(),
            edge_type: self.edge_type.clone(),
            from_timestamp: None,
            to_timestamp: None,
        }
    }
}

#[derive(
    Decode, Encode, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Debug,
)]
pub struct Node {
    pub query_node_name: Option<String>,
    pub instance_node_name: Option<String>,
    pub node_type: Option<String>,
    pub node_class: NodeClass,
    pub value_bytes: Option<Vec<u8>>,
}

impl Node {
    pub fn property_query_node(query_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: None,
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Property,
            value_bytes: None,
        }
    }
    pub fn property_instance_node(instance_node_name: &str, node_type: &str, value: Vec<u8>) -> Node {
        Node {
            query_node_name: None,
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Property,
            value_bytes: Some(value),
        }
    }

    pub fn object_query_node(query_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: None,
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Object,
            value_bytes: None,
        }
    }

    pub fn object_matched_query_node(query_node_name: &str, instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Object,
            value_bytes: None,
        }
    }

    pub fn object_instance_node(instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: None,
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Object,
            value_bytes: None,
        }
    }

    pub fn material_query_node(query_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: None,
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Material,
            value_bytes: None,
        }
    }

    pub fn material_matched_query_node(query_node_name: &str, instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Material,
            value_bytes: None,
        }
    }

    pub fn material_instance_node(instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: None,
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Material,
            value_bytes: None,
        }
    }

    pub fn event_query_node(query_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: None,
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Event,
            value_bytes: None,
        }
    }

    pub fn event_matched_query_node(query_node_name: &str, instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: Some(query_node_name.to_string()),
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Event,
            value_bytes: None,
        }
    }

    pub fn event_instance_node(instance_node_name: &str, node_type: &str) -> Node {
        Node {
            query_node_name: None,
            instance_node_name: Some(instance_node_name.to_string()),
            node_type: Some(node_type.to_string()),
            node_class: NodeClass::Event,
            value_bytes: None,
        }
    }

    pub fn forget_particular_material(&mut self) {
        if self.node_class == NodeClass::Material {
            self.query_node_name = None;
            self.instance_node_name = None;
        }
    }

    pub fn forget_particular_property(&mut self) {
        if self.node_class == NodeClass::Property {
            self.query_node_name = None;
            self.instance_node_name = None;
        }
    }

    pub fn forget_particular_value(&mut self) {
        self.value_bytes = None;
    }

    pub fn forget_query_node_name(&mut self) {
        self.query_node_name = None;
    }
}

#[derive(
    Encode, Decode, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, Clone, Debug,
)]
pub struct Edge {
    pub src: Node,
    pub trg: Node,
    pub edge_type: String,
    pub from_timestamp: Option<u64>,
    pub to_timestamp: Option<u64>,
}

impl Edge {
    pub fn without_timestamp(src: Node, trg: Node, edge_type: &str) -> Edge {
        Edge {
            src,
            trg,
            edge_type: edge_type.to_string(),
            from_timestamp: None,
            to_timestamp: None,
        }
    }

    pub fn forget_particulars(&mut self) {
        self.src.forget_particular_material();
        self.src.forget_particular_property();
        self.src.forget_particular_value();
        self.trg.forget_particular_material();
        self.trg.forget_particular_value();
        self.trg.forget_particular_property();
    }

    pub fn forget_query_node_name(&mut self) {
        self.src.forget_query_node_name();
        self.trg.forget_query_node_name();
    }

    pub fn stable_hash(&self, c: Configuration) -> u64 {
        hash(
            bincode::encode_to_vec(self, c)
                .expect("Encodable")
                .as_slice(),
        )
    }

    pub fn from_deltas(deltas: Vec<&&Delta>) -> Option<Edge> {
        assert!(!deltas.is_empty());
        let mut edge = deltas.get(0).unwrap().to_edge();
        let mut min_from = None;
        let mut min_to = None;
        for d in deltas {
            if d.delta_type == DeltaType::Addition
                && (min_from.is_none() || min_from.is_some() && min_from.unwrap() > d.timestamp)
            {
                min_from = Some(d.timestamp);
            }
            if d.delta_type == DeltaType::Removal
                && (min_to.is_none() || min_to.is_some() && min_to.unwrap() > d.timestamp)
            {
                min_to = Some(d.timestamp);
            }
        }
        return if min_from.is_none() {
            None
        } else {
            edge.from_timestamp = min_from;
            edge.to_timestamp = min_to;
            Some(edge)
        };
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Graph {
    pub edges: Vec<Edge>,
    pub incoming: BTreeMap<Node, Vec<Edge>>,
    pub outgoing: BTreeMap<Node, Vec<Edge>>,
}

impl Graph {
    pub fn from_edges(edges: Vec<Edge>) -> Graph {
        let mut incoming = BTreeMap::new();
        for e in &edges {
            if !incoming.contains_key(&e.trg) {
                incoming.insert(e.trg.clone(), vec![e.clone()]);
            } else {
                incoming.get_mut(&e.trg).unwrap().push(e.clone());
            }
            if !incoming.contains_key(&e.src) {
                incoming.insert(e.src.clone(), vec![]);
            }
        }
        let mut outgoing = BTreeMap::new();
        for e in &edges {
            if !outgoing.contains_key(&e.src) {
                outgoing.insert(e.src.clone(), vec![e.clone()]);
            } else {
                outgoing.get_mut(&e.src).unwrap().push(e.clone());
            }
            if !outgoing.contains_key(&e.trg) {
                outgoing.insert(e.trg.clone(), vec![]);
            }
        }
        Graph {
            edges: edges,
            incoming,
            outgoing,
        }
    }
}

pub fn edges_from_deltas(deltas: &Vec<&Delta>) -> Vec<Edge> {
    let grouped_deltas = group_distinct_delta_intervals(deltas);
    let mut edges = vec![];
    for g in grouped_deltas {
        let edge_opt = Edge::from_deltas(g);
        if edge_opt.is_some() {
            edges.push(edge_opt.unwrap());
        }
    }
    edges
}

fn group_distinct_delta_intervals<'a>(deltas: &'a Vec<&Delta>) -> Vec<Vec<&'a &'a Delta>> {
    let mut additions: Vec<&&Delta> = deltas
        .into_iter()
        .filter(|d| d.delta_type == DeltaType::Addition)
        .collect();
    let mut removals: Vec<&&Delta> = deltas
        .into_iter()
        .filter(|d| d.delta_type == DeltaType::Removal)
        .collect();
    additions.sort_by_key(|d| d.timestamp);
    removals.sort_by_key(|d| d.timestamp);

    let mut groups = vec![];
    let mut group_removal_timestamp = vec![];

    for r in removals {
        groups.push(vec![r]);
        group_removal_timestamp.push(r.timestamp.clone());
    }

    let mut open_group = vec![];
    'additions: for a in additions {
        for (i, g) in groups.iter_mut().enumerate() {
            if a.timestamp <= group_removal_timestamp.get(i).unwrap().clone() {
                g.push(a);
                continue 'additions;
            }
        }
        open_group.push(a);
    }

    if !open_group.is_empty() {
        groups.push(open_group);
    }
    groups
}

#[test]
fn test_edges_from_deltas_two_removals() {
    let my_barrel = Node::material_instance_node("MyBarrel0", "Barrel");
    let my_platform = Node::object_instance_node("MyPlatform0", "Platform");
    let deltas = vec![
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 2,
            delta_type: DeltaType::Removal
        },
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 3,
            delta_type: DeltaType::Removal
        },
        Delta {
            src: my_barrel.clone(),
            trg: my_platform.clone(),
            edge_type: "At".to_string(),
            timestamp: 0,
            delta_type: DeltaType::Addition
        }];
    let mut borrow_deltas = vec![];
    for d in &deltas {
        borrow_deltas.push(d);
    }

    let updated_edges_with_timestamp = edges_from_deltas(&borrow_deltas);

    let expected = vec![Edge { src: my_barrel.clone(), trg: my_platform.clone(), edge_type: "At".to_string(), from_timestamp: Some(0), to_timestamp: Some(2) }];
    assert_eq!(updated_edges_with_timestamp, expected);
}
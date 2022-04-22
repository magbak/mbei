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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::Path;

use bincode::{config::Configuration, Decode, Encode};
use seahash::hash;
use serde::{Deserialize, Serialize};
use serde_yaml;

#[cfg(test)]
use crate::graph::NodeClass;
use crate::graph::{Edge, Graph, Node};

#[derive(Encode, Decode, Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Query {
    pub name: String,
    pub application: String,
    pub graph: Graph,
    pub optional_edges: BTreeSet<Edge>,
    pub group: BTreeSet<Node>,
    pub output_edges: BTreeSet<Edge>,
    pub input_nodes: BTreeSet<Node>,
}

impl Query {
    pub fn create_matched_query(
        &self,
        grouped_query_match: GroupedQueryMatch,
        suffix: String,
    ) -> Query {
        assert!(grouped_query_match.grouped_matches.len() > 0);

        let image_homomorphism = grouped_query_match.get_image_homomorphism();
        let node_image_homomorphism = grouped_query_match.get_node_image_homomorphism();

        let mut new_edges = BTreeSet::new();
        let mut new_optional_edges = BTreeSet::new();
        let mut new_output_edges = BTreeSet::new();
        let mut new_input_nodes = BTreeSet::new();
        let mut new_group = BTreeSet::new();

        let mut add_edges_to_structure = |query_edge: &&Edge, edges_to_add: Vec<Edge>| {
            for m in &edges_to_add {
                new_edges.insert(m.clone());
            }
            if self.optional_edges.contains(query_edge) {
                for m in &edges_to_add {
                    new_optional_edges.insert(m.clone());
                }
            }
            if self.output_edges.contains(query_edge) {
                for m in &edges_to_add {
                    new_output_edges.insert(m.clone());
                }
            }
            if self.input_nodes.contains(&query_edge.src) {
                for m in &edges_to_add {
                    new_input_nodes.insert(m.src.clone());
                }
            }
            if self.input_nodes.contains(&query_edge.trg) {
                for m in &edges_to_add {
                    new_input_nodes.insert(m.trg.clone());
                }
            }
            if self.group.contains(&query_edge.src) {
                for m in &edges_to_add {
                    new_group.insert(m.src.clone());
                }
            }
            if self.group.contains(&query_edge.trg) {
                for m in &edges_to_add {
                    new_group.insert(m.trg.clone());
                }
            }
        };

        for (query_edge, matched_edges) in &image_homomorphism {
            let mut edges_to_add;
            if matched_edges.len() > 0 {
                edges_to_add = matched_edges
                    .iter()
                    .map(|x| (*x).clone())
                    .collect::<Vec<Edge>>();
            } else {
                let src_image = node_image_homomorphism.get(&query_edge.src).unwrap();
                let trg_image = node_image_homomorphism.get(&query_edge.trg).unwrap();
                edges_to_add = vec![];

                if src_image.len() > 0 {
                    if trg_image.len() > 0 {
                        for s in src_image {
                            for t in trg_image {
                                edges_to_add.push(Edge {
                                    src: (*s).clone(),
                                    trg: (*t).clone(),
                                    edge_type: query_edge.edge_type.clone(),
                                    from_timestamp: query_edge.from_timestamp.clone(),
                                    to_timestamp: query_edge.to_timestamp.clone(),
                                });
                            }
                        }
                    } else {
                        for s in src_image {
                            edges_to_add.push(Edge {
                                src: (*s).clone(),
                                trg: query_edge.trg.clone(),
                                edge_type: query_edge.edge_type.clone(),
                                from_timestamp: query_edge.from_timestamp.clone(),
                                to_timestamp: query_edge.to_timestamp.clone(),
                            });
                        }
                    }
                } else if trg_image.len() > 0 {
                    for t in trg_image {
                        edges_to_add.push(Edge {
                            src: query_edge.src.clone(),
                            trg: (*t).clone(),
                            edge_type: query_edge.edge_type.clone(),
                            from_timestamp: query_edge.from_timestamp.clone(),
                            to_timestamp: query_edge.to_timestamp.clone(),
                        });
                    }
                } else {
                    edges_to_add.push((*query_edge).clone());
                }
            }
            //Identity belongs to query nodes..
            for mut e in edges_to_add.iter_mut() {
                e.src.query_node_name = query_edge.src.query_node_name.clone();
                e.trg.query_node_name = query_edge.trg.query_node_name.clone();
            }
            add_edges_to_structure(query_edge, edges_to_add);
        }

        for e in &self.output_edges {
            if (!node_image_homomorphism.contains_key(&e.src)
                || node_image_homomorphism.get(&e.src).unwrap().is_empty())
                && (!node_image_homomorphism.contains_key(&e.trg)
                    || node_image_homomorphism.get(&e.trg).unwrap().is_empty())
            {
                new_output_edges.insert(e.clone());
            }
        }

        let query_graph = Graph::from_edges(new_edges.into_iter().collect());
        Query {
            name: self.name.clone() + &suffix,
            application: self.application.clone(),
            graph: query_graph,
            optional_edges: new_optional_edges,
            group: new_group,
            output_edges: new_output_edges,
            input_nodes: new_input_nodes,
        }
    }

    pub fn find_all_grouped_matches(&self, g: &Graph) -> Vec<GroupedQueryMatch> {
        let ungrouped_matches = self.find_all_matches(g);
        if self.group.is_empty() {
            return ungrouped_matches.into_iter().map(|m| GroupedQueryMatch{ grouped_matches: vec![m]}).collect();
        } else {
            let mut grouped_matches_map: BTreeMap<Vec<(Node, Node)>, Vec<QueryMatch>> =
                BTreeMap::new();
            for m in ungrouped_matches {
                let mut grouped_node_mapping: BTreeSet<(Node, Node)> = BTreeSet::new();
                for (q_edge, m_edge_opt) in m.homomorphism.iter() {
                    if self.group.contains(&q_edge.src) {
                        if let Some(m_edge) = m_edge_opt {
                            grouped_node_mapping.insert((q_edge.src.clone(), m_edge.src.clone()));
                        }
                    }
                    if self.group.contains(&q_edge.trg) {
                        if let Some(m_edge) = m_edge_opt {
                            grouped_node_mapping.insert((q_edge.trg.clone(), m_edge.trg.clone()));
                        }
                    }
                }
                let mut grouped_node_mapping: Vec<(Node, Node)> =
                    grouped_node_mapping.into_iter().collect();
                grouped_node_mapping.sort();
                if !grouped_matches_map.contains_key(&grouped_node_mapping) {
                    grouped_matches_map.insert(grouped_node_mapping, vec![m]);
                } else {
                    grouped_matches_map
                        .get_mut(&grouped_node_mapping)
                        .unwrap()
                        .push(m);
                }
            }
            return grouped_matches_map
                .into_values()
                .map(|m| GroupedQueryMatch { grouped_matches: m })
                .collect();
        }
    }

    pub fn find_all_matches(&self, g: &Graph) -> Vec<QueryMatch> {
        let mut matches: Vec<QueryMatch> = Vec::new();
        matches.push(QueryMatch {
            homomorphism: BTreeMap::new(),
        });
        //Matches only contain edges which are not matched yet, i.e. those without identities
        for e in &self.graph.edges {
            if e.src.instance_node_name == None || e.trg.instance_node_name == None {
                matches = self.extend_matches(e, matches, g);
            }
        }
        let mut excluded = BTreeSet::new();
        for i in 0..matches.len() {
            if !excluded.contains(&i) {
                let m = matches.get(i).unwrap();
                for j in (i + 1)..matches.len() {
                    if j != i && !excluded.contains(&j) {
                        let k = matches.get(j).unwrap();
                        let mut m_included_in_k = true;
                        for (src, trg) in &m.homomorphism {
                            if trg.is_some() && k.homomorphism.get(src).unwrap() != trg {
                                m_included_in_k = false;
                                break;
                            }
                        }
                        let mut k_included_in_m = true;
                        for (src, trg) in &k.homomorphism {
                            if trg.is_some() && m.homomorphism.get(src).unwrap() != trg {
                                k_included_in_m = false;
                                break;
                            }
                        }
                        if k_included_in_m && !m_included_in_k {
                            excluded.insert(j);
                        } else if m_included_in_k && !k_included_in_m {
                            excluded.insert(i);
                        } else if m_included_in_k && k_included_in_m {
                            excluded.insert(i);
                        }
                    }
                }
            }
        }
        let mut non_redundant_matches = vec![];
        let mut i = 0;
        for m in matches {
            if !excluded.contains(&i) {
                non_redundant_matches.push(m);
            }
            i += 1;
        }

        if non_redundant_matches.len() == 1 {
            let mut nonempty: Vec<QueryMatch> = Vec::new();
            for m in non_redundant_matches {
                if !m.homomorphism.values().all(|v| v.is_none()) {
                    nonempty.push(m);
                }
            }
            return nonempty;
        } else {
            return non_redundant_matches;
        }
    }

    pub fn extend_matches(&self, e: &Edge, matches: Vec<QueryMatch>, g: &Graph) -> Vec<QueryMatch> {
        let edge_filter = |t: &&Edge| {
            (t.edge_type == e.edge_type)
                && (e.src.node_class == t.src.node_class)
                && (e.trg.node_class == t.trg.node_class)
                && (e.src.node_type.is_none() || (e.src.node_type == t.src.node_type))
                && (e.trg.node_type.is_none() || (e.trg.node_type == t.trg.node_type))
                && (e.src.instance_node_name.is_none()
                    || (e.src.instance_node_name == t.src.instance_node_name))
                && (e.trg.instance_node_name.is_none()
                    || (e.trg.instance_node_name == t.trg.instance_node_name))
        };
        let candidates: Vec<&Edge> = g.edges.iter().filter(edge_filter).collect();
        let mut new_matches: Vec<QueryMatch> = Vec::new();
        for m in matches {
            'cloop: for c in candidates.iter() {
                if e.src.instance_node_name.is_none() {
                    for incoming_src in &self.graph.incoming[&e.src] {
                        if incoming_src != e && m.homomorphism.contains_key(&incoming_src) {
                            if let Some(mapped_incoming_src) =
                                m.homomorphism.get(&incoming_src).unwrap()
                            {
                                // a --d-->b--e-->h => f(a) --f(d)-->f(b)--f(e)-->f(h)
                                if mapped_incoming_src.trg != c.src {
                                    continue 'cloop;
                                }
                            }
                        }
                    }
                    for outgoing_src in &self.graph.outgoing[&e.src] {
                        if outgoing_src != e && m.homomorphism.contains_key(&outgoing_src) {
                            if let Some(mapped_outgoing_src) =
                                m.homomorphism.get(&outgoing_src).unwrap()
                            {
                                // a<--d--b--e-->h => f(a)<--f(d)--f(b)--f(e)-->f(h)
                                if mapped_outgoing_src.src != c.src {
                                    continue 'cloop;
                                }
                            }
                        }
                    }
                }

                if e.trg.instance_node_name.is_none() {
                    for incoming_trg in &self.graph.incoming[&e.trg] {
                        if incoming_trg != e && m.homomorphism.contains_key(&incoming_trg) {
                            if let Some(mapped_incoming_trg) =
                                m.homomorphism.get(&incoming_trg).unwrap()
                            {
                                // a--d-->b<--e--h => f(a)--f(d)-->f(b)<--f(e)--f(h)
                                if mapped_incoming_trg.trg != c.trg {
                                    continue 'cloop;
                                }
                            }
                        }
                    }
                    for outgoing_trg in &self.graph.outgoing[&e.trg] {
                        if outgoing_trg != e && m.homomorphism.contains_key(&outgoing_trg) {
                            if let Some(mapped_outgoing_trg) =
                                m.homomorphism.get(&outgoing_trg).unwrap()
                            {
                                // a<--d--b<--e--h => f(a)<--f(d)--f(b)<--f(e)--f(h)
                                if mapped_outgoing_trg.src != c.trg {
                                    continue 'cloop;
                                }
                            }
                        }
                    }
                }

                let mut new_match = QueryMatch {
                    homomorphism: m.homomorphism.clone(),
                };
                new_match.homomorphism.insert(e.clone(), Some((*c).clone()));
                new_matches.push(new_match);
            }
            if self.optional_edges.contains(e) {
                let mut empty_extension = QueryMatch {
                    homomorphism: m.homomorphism.clone(),
                };
                empty_extension.homomorphism.insert(e.clone(), None);
                new_matches.push(empty_extension);
            }
        }
        new_matches
    }

    pub fn from_bytestring(s: &String, config: Configuration) -> Query {
        let b = s.as_bytes();
        let (query, _) = bincode::decode_from_slice(b, config).expect("Decoding error");
        query
    }

    pub fn to_bytestring(&self, config: Configuration) -> String {
        String::from_utf8(bincode::encode_to_vec(self, config).expect("Encoding error"))
            .expect("Utf8err")
    }
}

#[derive(Encode, Debug, PartialEq)]
pub struct QueryMatch {
    pub homomorphism: BTreeMap<Edge, Option<Edge>>,
}

#[derive(Encode, Debug)]
pub struct GroupedQueryMatch {
    pub grouped_matches: Vec<QueryMatch>,
}

impl GroupedQueryMatch {
    pub(crate) fn get_image_homomorphism(&self) -> BTreeMap<&Edge, Vec<Edge>> {
        let mut image_homomorphism = BTreeMap::new();
        for query_match in &self.grouped_matches {
            for (query_edge, matched_edge_opt) in &query_match.homomorphism {
                if !image_homomorphism.contains_key(query_edge) {
                    image_homomorphism.insert(query_edge, vec![]);
                }
                if let Some(matched_edge) = matched_edge_opt {
                    image_homomorphism
                        .get_mut(query_edge)
                        .unwrap()
                        .push(matched_edge.clone());
                }
            }
        }
        image_homomorphism
    }

    pub(crate) fn get_node_image_homomorphism(&self) -> BTreeMap<&Node, BTreeSet<&Node>> {
        let mut node_image_homomorphism = BTreeMap::new();
        for query_match in &self.grouped_matches {
            for (query_edge, matched_edge_opt) in &query_match.homomorphism {
                if !node_image_homomorphism.contains_key(&query_edge.src) {
                    node_image_homomorphism.insert(&query_edge.src, BTreeSet::new());
                }
                if !node_image_homomorphism.contains_key(&query_edge.trg) {
                    node_image_homomorphism.insert(&query_edge.trg, BTreeSet::new());
                }
                if let Some(matched_edge) = matched_edge_opt {
                    node_image_homomorphism
                        .get_mut(&query_edge.src)
                        .unwrap()
                        .insert(&matched_edge.src);
                    node_image_homomorphism
                        .get_mut(&query_edge.trg)
                        .unwrap()
                        .insert(&matched_edge.trg);
                }
            }
        }
        node_image_homomorphism
    }

    pub fn stable_hash(&self, c: Configuration) -> u64 {
        hash(
            bincode::encode_to_vec(self, c)
                .expect("Encodable")
                .as_slice(),
        )
    }
}

pub fn parse_queries(p: &Path, config: Configuration) -> Vec<Query> {
    let queries_file_reader = File::open(p).expect("File exists");
    let queries_enc: HashMap<String, String> =
        serde_yaml::from_reader(queries_file_reader).expect("Can parse YAML");
    queries_enc
        .iter()
        .map(|(_, qstring)| Query::from_bytestring(qstring, config))
        .collect()
}

#[test]
fn test_stable_hash() {
    let n1 = Node {
        query_node_name: Option::from("i1".to_string()),
        instance_node_name: Option::from("nn1".to_string()),
        node_type: Option::from("nt1".to_string()),
        node_class: NodeClass::Event,
        value_bytes: None,
    };
    let n2 = Node {
        query_node_name: Option::from("i2".to_string()),
        instance_node_name: Option::from("nn2".to_string()),
        node_type: Option::from("nt2".to_string()),
        node_class: NodeClass::Object,
        value_bytes: None,
    };
    let n3 = Node {
        query_node_name: Option::from("i3".to_string()),
        instance_node_name: Option::from("nn3".to_string()),
        node_type: Option::from("nt3".to_string()),
        node_class: NodeClass::Material,
        value_bytes: None,
    };
    let n4 = Node {
        query_node_name: Option::from("i4".to_string()),
        instance_node_name: Option::from("nn4".to_string()),
        node_type: Option::from("nt4".to_string()),
        node_class: NodeClass::Object,
        value_bytes: None,
    };
    let e1 = Edge {
        src: n1,
        trg: n2,
        edge_type: "At".to_string(),
        from_timestamp: None,
        to_timestamp: None,
    };
    let e2 = Edge {
        src: n3,
        trg: n4,
        edge_type: "At".to_string(),
        from_timestamp: None,
        to_timestamp: None,
    };

    let mut qm = QueryMatch {
        homomorphism: BTreeMap::new(),
    };
    qm.homomorphism.insert(e1.clone(), Some(e2.clone()));
    let gqm = GroupedQueryMatch {
        grouped_matches: vec![qm],
    };

    let c = bincode::config::standard();
    assert_eq!(gqm.stable_hash(c), 1893789860135764682);
}

#[test]
fn pickdrop_query_matching() {
    let trg = Edge::without_timestamp(
        Node::material_instance_node("MyBarrel0", "Barrel"),
        Node::object_instance_node("MyPlatform0", "Platform"),
        "At",
    );
    let graph = Graph::from_edges(vec![trg.clone()]);

    //Nodes
    let c = Node::object_matched_query_node("c", "MyCrane0", "Crane");
    let o_plat = Node::object_matched_query_node("o", "MyPlatform0", "Platform");
    let bo = Node::material_query_node("bo", "Barrel");
    let bc = Node::material_query_node("bo", "Barrel");
    let oap_plat =
        Node::object_matched_query_node("oap", "MyPlatform0.ObjectAtPosition", "ObjectAtPosition");
    let oap_ramp =
        Node::object_matched_query_node("oap", "MyRamp0.ObjectAtPosition", "ObjectAtPosition");
    let r = Node::object_matched_query_node("o", "MyRamp0", "Ramp");

    //Edges
    let bc_at_c = Edge::without_timestamp(bc.clone(), c.clone(), "At");
    let bo_at_o_plat = Edge::without_timestamp(bo.clone(), o_plat.clone(), "At");
    let bo_at_o_ramp = Edge::without_timestamp(bo.clone(), r.clone(), "At");
    let oap_has_plat = Edge::without_timestamp(oap_plat.clone(), o_plat.clone(), "HasObject");
    let oap_has_ramp = Edge::without_timestamp(oap_ramp.clone(), r.clone(), "HasObject");
    let c_has_oap_ramp =
        Edge::without_timestamp(c.clone(), oap_plat.clone(), "HasObjectAtPosition");
    let c_has_oap_plat =
        Edge::without_timestamp(c.clone(), oap_ramp.clone(), "HasObjectAtPosition");
    let pickdrop = Node::event_instance_node("MyCrane0.PickDrop", "PickDrop");
    let c_has_p = Edge::without_timestamp(c.clone(), pickdrop.clone(), "HasEvent");

    let query = Query {
        name: "pickdrop_matched".to_string(),
        application: "pickdrop".to_string(),
        graph: Graph::from_edges(vec![
            bc_at_c.clone(),
            bo_at_o_plat.clone(),
            bo_at_o_ramp.clone(),
            oap_has_ramp,
            oap_has_plat,
            c_has_oap_ramp,
            c_has_oap_plat,
            c_has_p,
        ]),
        optional_edges: BTreeSet::from([
            bc_at_c.clone(),
            bo_at_o_plat.clone(),
            bo_at_o_ramp.clone(),
        ]),
        group: BTreeSet::from([Node::object_matched_query_node("c", "MyCrane0", "Crane")]),
        output_edges: BTreeSet::from([bc_at_c.clone(), bo_at_o_plat.clone(), bo_at_o_ramp.clone()]),
        input_nodes: BTreeSet::from([pickdrop.clone()]),
    };

    let matches = query.find_all_matches(&graph);
    assert_eq!(matches.len(), 1);
    let expected_matches = vec![QueryMatch {
        homomorphism: BTreeMap::from([
            (bc_at_c.clone(), None),
            (bo_at_o_plat.clone(), Some(trg.clone())),
            (bo_at_o_ramp.clone(), None),
        ]),
    }];
    assert_eq!(matches, expected_matches);
}

#[test]
fn test_no_groupby() {
    let my_platform_query = Node::object_matched_query_node("p","MyPlatform5", "Platform");
    let my_platform_instance = Node::object_instance_node("MyPlatform5", "Platform");
    let barrel_query = Node::material_query_node("b", "Barrel");
    let detector_query = Node::event_matched_query_node("d", "MyDetector5", "Detector");
    let q = Query {
        name: "detector_matched_5".to_string(),
        application: "detector".to_string(),
        graph: Graph::from_edges(vec![
            Edge {
                src: barrel_query,
                trg: my_platform_query.clone(),
                edge_type: "At".to_string(),
                from_timestamp: None,
                to_timestamp: None,
            },
            Edge {
                src: my_platform_query.clone(),
                trg: detector_query.clone(),
                edge_type: "HasEvent".to_string(),
                from_timestamp: None,
                to_timestamp: None,
            },
        ]),
        optional_edges: BTreeSet::new(),
        group: BTreeSet::new(),
        output_edges: BTreeSet::new(),
        input_nodes: BTreeSet::from([detector_query]),
    };
    let my_barrel1 = Node::material_instance_node("MyBarrel1", "Barrel");
    let my_barrel2 = Node::material_instance_node("MyBarrel2", "Barrel");
    let gr = Graph::from_edges(vec![
        Edge::without_timestamp(my_barrel1, my_platform_instance.clone(), "At"),
        Edge::without_timestamp(my_barrel2, my_platform_instance.clone(), "At")
    ]);

    let matches = q.find_all_grouped_matches(&gr);
    assert_eq!(matches.len(), 2);
}

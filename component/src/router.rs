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

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use log::debug;
use tokio::task::JoinHandle;
use tonic::{Response, Status};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::store::TopicNameAndDeltasId;
use mbei_core::event::{Deltas, Retractions, Update};
use mbei_core::graph::Edge;
use mbei_core::query::Query;

use mbei_grpc::process_update::process_update_client::ProcessUpdateClient;
use mbei_grpc::process_update::{ProcessUpdateRequest, ProcessUpdateResponse};
use mbei_grpc::process_update_client::create_process_update_client;
use mbei_grpc::process_update_mapping::request_from_update;

type JoinHandleType = JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>;

pub struct Router {
    pub query_name: String,
    edge_forward_map: BTreeMap<Edge, BTreeSet<String>>,
    reached_set: BTreeSet<String>,
    client_map: BTreeMap<String, ProcessUpdateClient<tonic::transport::Channel>>,
    query_url_map: BTreeMap<String, String>,
    use_central: bool
}

impl Router {
    pub fn new(
        query_name: String,
        all_queries_by_name: BTreeMap<String, Query>,
        query_url_map: BTreeMap<String, String>,
        use_central: bool
    ) -> Router {
        let mut all_edge_forward_maps =
            Router::compute_edge_forward_maps(&all_queries_by_name);
        all_edge_forward_maps = Router::compute_edge_forward_closure(all_edge_forward_maps, &all_queries_by_name);
        let owned_edge_forward_map = all_edge_forward_maps.remove(&query_name).unwrap();
        let mut reached_set = BTreeSet::new();
        for qnames in owned_edge_forward_map.values() {
            for qname in qnames {
                reached_set.insert(qname.clone());
            }
        }
        debug!("{} forward map {:?}", &query_name, &owned_edge_forward_map);
        Router {
            query_name,
            edge_forward_map:owned_edge_forward_map,
            reached_set,
            query_url_map,
            client_map: BTreeMap::new(),
            use_central
        }
    }

    pub(crate) async fn start(&mut self, max_elapsed_time: Option<Duration>) {
        debug!("Starting router");
        let mut handle_map = BTreeMap::new();
        for (q, url) in &self.query_url_map {
            if q == "central" || self.reached_set.contains(q) {
                handle_map.insert(q, tokio::spawn(create_process_update_client(url.clone(), max_elapsed_time.clone())));
            }
        }
        let mut client_map = BTreeMap::new();
        for (q, h) in handle_map {
            client_map.insert(q.clone(), h.await.expect("create client error").expect("create client error"));
        }
        self.client_map = client_map;
        debug!("Router started");
    }

    //Computes triangle and transitive closure..
    pub fn compute_edge_forward_closure(mut edge_forward_maps:BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>>, all_queries_by_name: &BTreeMap<String, Query>) -> BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>> {
        edge_forward_maps = Router::compute_edge_forward_transitive_closure(edge_forward_maps, all_queries_by_name);
        Router::compute_edge_forward_triangle_closure(edge_forward_maps, all_queries_by_name)
    }

    fn compute_edge_forward_transitive_closure(mut edge_forward_maps:BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>>, all_queries_by_name: &BTreeMap<String, Query>) -> BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>> {
        let mut all_reachable = Router::compute_reachable_map(&edge_forward_maps);

        let mut new_reachable = all_reachable.clone();
        while !new_reachable.is_empty() {
            let mut iter_reachable = BTreeMap::new();
            for (q, rs) in &new_reachable {
                let forward_map = edge_forward_maps.get_mut(q).unwrap();
                let q_query = all_queries_by_name.get(q).unwrap();
                let existing_reachable_set = all_reachable.get(q).unwrap();
                for r in rs {
                    for p in all_reachable.get(r).unwrap() {
                        if !existing_reachable_set.contains(p) {
                            let p_query = all_queries_by_name.get(p).unwrap();
                            let include_edges = Router::find_output_edges(q_query, p_query);
                            if !include_edges.is_empty() {
                                for e in include_edges {
                                    if !forward_map.contains_key(&e) {
                                        forward_map.insert(e.clone(), BTreeSet::from([p.clone()]));
                                    } else {
                                        forward_map.get_mut(&e).unwrap().insert(p.clone());
                                    }
                                    if !iter_reachable.contains_key(q) {
                                        iter_reachable.insert(q.clone(), BTreeSet::from([p.clone()]));
                                    } else {
                                        iter_reachable.get_mut(&*q.clone()).unwrap().insert(p.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for (q, rs) in &iter_reachable {
                let reachable_set = all_reachable.get_mut(q).unwrap();
                for r in rs {
                    reachable_set.insert(r.clone());
                }
            }
            new_reachable = iter_reachable;
        }
        edge_forward_maps
    }

    //a--->b and a--->c => possibly b--->c but only if b produces output of interest for c
    fn compute_edge_forward_triangle_closure(mut edge_forward_maps:BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>>, all_queries_by_name: &BTreeMap<String, Query>) -> BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>> {
        let all_reachable = Router::compute_reachable_map(&edge_forward_maps);
        for (_, rs) in &all_reachable {
            for r1 in rs {
                let forward_map = edge_forward_maps.get_mut(r1).unwrap();
                let r1_query = all_queries_by_name.get(r1).unwrap();
                for r2 in rs {
                    if r1 != r2 {
                        let r2_query = all_queries_by_name.get(r2).unwrap();
                        let include_edges = Router::find_output_edges(r1_query, r2_query);
                        if !include_edges.is_empty() {
                            for e in include_edges {
                                if !forward_map.contains_key(&e) {
                                    forward_map.insert(e.clone(), BTreeSet::from([r2.clone()]));
                                } else {
                                    forward_map.get_mut(&e).unwrap().insert(r2.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
        edge_forward_maps
    }


    pub fn compute_reachable_map(edge_forward_maps:&BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>>) -> BTreeMap<String, BTreeSet<String>> {
        let mut reachable_maps = BTreeMap::new();
        for (q, edge_forward_map) in edge_forward_maps {
            let mut reached = BTreeSet::new();
            for v in edge_forward_map.values() {
                for s in v {
                    reached.insert(s.clone());
                }
            }
            reachable_maps.insert(q.clone(), reached);
        }
        reachable_maps
    }

    pub fn compute_edge_forward_maps(
        all_queries_by_name: &BTreeMap<String, Query>,
    ) -> BTreeMap<String, BTreeMap<Edge, BTreeSet<String>>> {
        let mut all_maps = BTreeMap::new();
        for q in all_queries_by_name.keys() {
            all_maps.insert(
                q.clone(),
                Router::compute_edge_forward_map(q.clone(), all_queries_by_name),
            );
        }
        all_maps
    }

    fn compute_edge_forward_map(
        query_name: String,
        all_queries_by_name: &BTreeMap<String, Query>,
    ) -> BTreeMap<Edge, BTreeSet<String>> {
        let my_query = all_queries_by_name.get(&query_name).unwrap();
        let mut edge_forward_map = BTreeMap::new();
        for o in my_query.output_edges.iter() {
            let mut o_without_query_name = o.clone();
            o_without_query_name.forget_query_node_name();
            edge_forward_map.insert(o_without_query_name, BTreeSet::new());
        }
        for (query_name, query) in all_queries_by_name {
            let output_edges = Router::find_output_edges(my_query, query);
            for o in output_edges {
                let mut o_without_query_name = o.clone();
                o_without_query_name.forget_query_node_name();
                edge_forward_map
                    .get_mut(&o_without_query_name)
                    .unwrap()
                    .insert(query_name.clone());
            }
        }
        edge_forward_map
    }

    fn find_output_edges<'a>(src_query:&'a Query, trg_query:&Query) -> Vec<&'a Edge> {
        let mut edges = vec![];
        'outer: for o in src_query.output_edges.iter() {
            for i in trg_query.graph.edges.iter() {
                let src_match = (o.src.node_class == i.src.node_class)
                    && (i.src.node_type.is_none() || i.src.node_type == o.src.node_type)
                    && (i.src.instance_node_name.is_none()
                    || i.src.instance_node_name == o.src.instance_node_name);
                let trg_match = (o.trg.node_class == i.trg.node_class)
                    && (i.trg.node_type.is_none() || i.trg.node_type == o.trg.node_type)
                    && (i.trg.instance_node_name.is_none()
                    || i.trg.instance_node_name == o.trg.instance_node_name);
                if (src_match) && (trg_match) && i.edge_type == o.edge_type {
                    edges.push(o);
                    continue 'outer;
                }
            }
        }
        edges
    }

    pub(crate) async fn route_retractions(
        &self,
        query_names_and_update_ids: Vec<TopicNameAndDeltasId>,
        event_timestamp: &u64,
    ) -> (Vec<Update>, Vec<JoinHandleType>) {
        let mut internal_updates = vec![];
        let mut handles = vec![];
        for qnid in query_names_and_update_ids {
            let retraction_update = Update::Retractions(Retractions {
                retraction_id: Uuid::new_v4().to_hyphenated().to_string(),
                timestamp: event_timestamp.clone(),
                deltas_ids: vec![qnid.deltas_id.clone()],
            });
            if &qnid.topic_name == &self.query_name {
                internal_updates.push(retraction_update);
            } else {
                let handle = self
                    .send_update(&retraction_update, &qnid.topic_name)
                    .await;
                handles.push(handle);
            }
        }
        (internal_updates, handles)
    }

    async fn send_update(
        &self,
        update: &Update,
        send_to_query_name: &str,
    ) -> JoinHandleType {
        debug!("{} sending update to {}", &self.query_name, &send_to_query_name);
        let client = self.client_map.get(send_to_query_name).unwrap().clone();
        let request = request_from_update(update);

        let handle = tokio::spawn(Router::owning_send(client, request
        ));
        handle
    }

    async fn owning_send(mut client:ProcessUpdateClient<Channel>, request:ProcessUpdateRequest) -> Result<Response<ProcessUpdateResponse>, Status> {
        client.send(request).await
    }

    async fn send_central_update(&self, update: &Update) -> JoinHandleType {
        self.send_update(&update, "central").await
    }

    pub(crate) async fn route_deltas_update(
        &self,
        deltas: Deltas,
    ) -> (
        Option<Update>,
        Vec<TopicNameAndDeltasId>,
        Vec<JoinHandleType>,
    ) {
        //Initialize output values
        let mut handles = vec![];
        let mut cascaded_topic_and_deltas_ids = vec![];

        if self.use_central {
            //First we route to central store
            debug!(
            "{} sent deltas with id {} and event id {} to central",
            &self.query_name, &deltas.deltas_id, &deltas.origin_id
        );
            let central_handle = self
                .send_central_update(&Update::Deltas(deltas.clone()))
                .await;
            let central_cascaded = TopicNameAndDeltasId {
                topic_name: "central".to_string(),
                deltas_id: deltas.deltas_id.clone(),
            };
            handles.push(central_handle);
            cascaded_topic_and_deltas_ids.push(central_cascaded);
        }

        let mut internal_update = None;
        let mut query_deltas = BTreeMap::new();
        let mut edge_without_particulars;
        let mut queries;
        for d in deltas.deltas.iter() {
            edge_without_particulars = d.to_edge();
            edge_without_particulars.forget_particulars();
            queries = self.edge_forward_map.get(&edge_without_particulars);
            if queries.is_some() {
                for q in queries.unwrap() {
                    if !query_deltas.contains_key(q) {
                        query_deltas.insert(q, vec![]);
                    }
                    query_deltas.get_mut(q).unwrap().push(d.clone());
                }
            }
        }
        let mut new_update;
        for (q, ds) in query_deltas {
            let deltas_id = Uuid::new_v4().to_hyphenated().to_string();
            new_update = Update::Deltas(Deltas {
                deltas_id: deltas_id.clone(),
                origin_id: deltas.origin_id.clone(),
                origin_timestamp: deltas.origin_timestamp.clone(),
                deltas: BTreeSet::from_iter(ds.into_iter()),
            });
            cascaded_topic_and_deltas_ids.push(TopicNameAndDeltasId {
                topic_name: q.clone(),
                deltas_id: deltas_id.clone(),
            });
            if q == &self.query_name {
                debug!(
                    "{} routed update {} internally",
                    &self.query_name, &deltas_id
                );
                internal_update = Some(new_update);
            } else {
                debug!(
                    "{} sent deltas with id {} and event id {} to {}",
                    &self.query_name, &deltas_id, &deltas.origin_id, &q
                );
                let handle = self.send_update(&new_update, q).await;
                handles.push(handle);
                //TODO: Add retraction possibility to store..
            }
        }
        (internal_update, cascaded_topic_and_deltas_ids, handles)
    }
}

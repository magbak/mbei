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

use bincode::config::{standard, Configuration};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use log::{debug, info};
use tokio::task::JoinHandle;
use tonic::{Response, Status};

use mbei_core::event::{Deltas, Event, Retractions, Update};
use mbei_core::graph::{edges_from_deltas, Delta, Edge, Graph};
use mbei_core::query::{GroupedQueryMatch, Query};
use mbei_grpc::process_update::ProcessUpdateResponse;

use crate::caller::Caller;
use crate::intervals::{
    find_intervals_to_reprocess, find_non_redundant_intervals, ReprocessInterval,
};
use crate::router::Router;
use crate::store::{DeltaAndDeltasId, Store};

pub struct Component {
    store: Store,
    caller: Caller,
    router: Router,
    query: Query,
    config: Configuration,
}

impl Component {
    pub fn new(
        query_name: String,
        all_queries_by_name: BTreeMap<String, Query>,
        application_grpc_url: String,
        query_url_map: BTreeMap<String, String>,
        use_central: bool,
    ) -> Component {
        Component {
            store: Store::new(),
            caller: Caller::new(application_grpc_url),
            query: all_queries_by_name.get(&query_name).unwrap().clone(),
            router: Router::new(query_name, all_queries_by_name, query_url_map, use_central),
            config: standard(),
        }
    }

    pub(crate) async fn start(&mut self, max_elapsed_time: Option<Duration>) {
        self.caller.start(max_elapsed_time).await;
        self.router.start(max_elapsed_time).await;
    }

    pub(crate) fn stop(&mut self) {}

    pub(crate) async fn process_update_until_consistency(
        &mut self,
        update: Update,
    ) -> (Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>, i32, i32, i32, i32, usize) {
        if let Update::Deltas(deltas) = &update {
            let is_retracted = self.store.is_update_rectracted(&deltas.deltas_id);
            if is_retracted {
                return (vec![], 0, 0, 0, 0, self.store.open_edges.len());
            }
        }

        let mut updates_to_process = vec![update];
        let mut handles = vec![];
        let mut retracted_ids = BTreeSet::new();
        let mut seq = 0;
        let mut reprocess_intervals = vec![];

        let mut n_deltas = 0;
        let mut n_events = 0;
        let mut n_retractions = 0;
        let mut n_reprocessing = 0;

        while !updates_to_process.is_empty() || !reprocess_intervals.is_empty() {
            if seq > 1000 {
                debug!("Reached over 100 iterations, {:?}", &updates_to_process);
                panic!("Over 100 loops");
            }
            let update = updates_to_process.pop().unwrap();

            match update {
                Update::Event(event) => {
                    info!(
                        "{} processing event with id {}",
                        &self.query.name, &event.event_id
                    );
                    if !retracted_ids.contains(&event.event_id) {
                        let (mut new_updates, mut new_handles) =
                            self.process_new_event(&event).await;
                        updates_to_process.append(&mut new_updates);
                        handles.append(&mut new_handles);
                        n_events += 1;
                    } else {
                        info!(
                            "{} event with id {} was retracted, doing nothing",
                            &self.query.name, &event.event_id
                        );
                    }
                }
                Update::Deltas(deltas) => {
                    info!(
                        "{} processing deltas with id {} and event id {}",
                        &self.query.name, &deltas.deltas_id, &deltas.origin_id
                    );
                    if !retracted_ids.contains(&deltas.deltas_id)
                        && !retracted_ids.contains(&deltas.origin_id)
                    {
                        let mut new_reprocess_intervals = self.process_new_deltas(&deltas);
                        reprocess_intervals.append(&mut new_reprocess_intervals);
                        n_deltas += 1;
                    } else {
                        info!(
                            "{} deltas with id {} and event id {} was retracted, doing nothing",
                            &self.query.name, &deltas.deltas_id, &deltas.origin_id
                        );
                    }
                }
                Update::Retractions(retractions) => {
                    info!(
                        "{} processing retractions with id {} ",
                        &self.query.name, &retractions.retraction_id
                    );
                    for id in &retractions.deltas_ids {
                        retracted_ids.insert(id.clone());
                    }
                    let mut new_reprocess_intervals = self.process_retractions(retractions);
                    reprocess_intervals.append(&mut new_reprocess_intervals);
                    n_retractions += 1;
                }
                _ => {
                    panic!("Should never happen")
                }
            };

            if updates_to_process.is_empty() && !reprocess_intervals.is_empty() {
                let (mut new_updates, mut new_handles, new_reprocess_intervals, n_events) = self
                    .reprocess_events_in_intervals_until_internal_update(reprocess_intervals)
                    .await;
                reprocess_intervals = new_reprocess_intervals;
                updates_to_process.append(&mut new_updates);
                handles.append(&mut new_handles);
                n_reprocessing += n_events;
            }

            updates_to_process.sort_by_key(|u| if let Update::Retractions(_) = u { 0 } else { 1 });
            seq += 1;
        }
        (handles, n_deltas, n_events, n_retractions, n_reprocessing, self.store.open_edges.len())
    }

    fn process_retractions(&mut self, retractions: Retractions) -> Vec<ReprocessInterval> {
        //First we retract deltas contained in the updates and the new updates based on matches which are now lost
        self.store.add_retractions(&retractions.deltas_ids);
        let deltas_to_retract = self.store.pop_deltas_by_deltas_ids(&retractions.deltas_ids);
        debug!(
            "{} found {} deltas to retract for retraction {} with deltas id {:?}",
            &self.query.name,
            deltas_to_retract.len(),
            &retractions.retraction_id,
            &retractions.deltas_ids
        );
        if !deltas_to_retract.is_empty() {
            self.retract_deltas(deltas_to_retract)
        } else {
            vec![]
        }
    }

    fn process_new_deltas(&mut self, deltas: &Deltas) -> Vec<ReprocessInterval> {
        let now = Instant::now();
        let new_deltas_by_edge = get_deltas_vec_by_edge(deltas.deltas.iter().collect());
        let new_deltas_edges_vec: Vec<&Edge> = new_deltas_by_edge.keys().collect();
        let existing_deltas_by_edge = self.store.get_deltas_by_edge_vec(new_deltas_edges_vec);
        let updated_deltas_by_edge = self
            .store
            .add_deltas_and_get_updated_deltas_by_edge(&deltas);
        info!(
            "{} new deltas processing took {} μs",
            &self.query.name,
            now.elapsed().as_micros()
        );
        self.update_edges_and_find_intervals_to_reprocess(
            existing_deltas_by_edge,
            updated_deltas_by_edge,
        )
    }

    //Idempotent operation..
    async fn process_event(
        &mut self,
        event: &Event,
    ) -> (
        Vec<Update>,
        Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>,
    ) {
        let now = Instant::now();
        debug!(
            "{} processing event {:?} with timestamp {:?}",
            &self.query.name, &event.event_id, &event.timestamp
        );
        let mut all_updates = vec![];
        let mut all_handles = vec![];

        let mut all_edges = self.store.get_edges_at_timestamp(event.timestamp);
        debug!(
            "{} There are {} edges at timestamp {}",
            &self.query.name,
            &all_edges.len(),
            event.timestamp
        );

        let matches;
        if !all_edges.is_empty() {
            //We are not allowed to look into the future
            for e in all_edges.iter_mut() {
                e.to_timestamp = None
            }

            let graph = Graph::from_edges(all_edges);
            matches = self.query.find_all_grouped_matches(&graph);
        } else {
            matches = vec![];
        }

        debug!("{} found {} matches", &self.query.name, matches.len());
        let matches_by_hash =
            BTreeMap::from_iter(matches.into_iter().map(|m| (m.stable_hash(self.config), m)));
        let existing_output_hashes_by_match_hashes = self
            .store
            .get_event_output_hash_by_match_hash(&event.event_id);
        let existing_match_hashes_by_output_hashes = BTreeMap::from_iter(
            existing_output_hashes_by_match_hashes
                .iter()
                .filter(|(_m, o)| o.is_some())
                .map(|(m, o)| (o.unwrap(), m.clone())),
        );

        let mut matches_to_possibly_retract = BTreeSet::new();
        for h in existing_output_hashes_by_match_hashes.keys() {
            if !matches_by_hash.contains_key(h) {
                matches_to_possibly_retract.insert(h.clone());
            }
        }

        let mut new_output_hashes_by_match_hashes = BTreeMap::new();
        for (match_hash, grouped_match) in matches_by_hash.iter() {
            if !existing_output_hashes_by_match_hashes.contains_key(match_hash) {
                debug!(
                    "{} found new match {} for event {}, computing output",
                    &self.query.name, match_hash, &event.event_id
                );
                let new_deltas_opt = self.process_new_match(grouped_match, event).await;
                if let Some(new_deltas) = new_deltas_opt {
                    let new_output_hash = new_deltas.stable_hash(self.config);
                    new_output_hashes_by_match_hashes
                        .insert(match_hash.clone(), Some(new_output_hash));
                    if let Some(old_match_hash) =
                        existing_match_hashes_by_output_hashes.get(&new_output_hash)
                    {
                        debug!(
                            "{} output already exists for new match {} event {}",
                            &self.query.name, match_hash, &event.event_id
                        );
                        //We will only relink the store so that the new match is linked to the old update
                        self.store.replace_old_match_with_equivalent_new_match(
                            &event.event_id,
                            old_match_hash,
                            match_hash,
                        );
                        let removed = matches_to_possibly_retract.remove(&old_match_hash);
                        //Else we have a duplicate match
                        assert!(removed);
                    } else {
                        debug!(
                            "{} output did not exist for new match {} event {}, creating update",
                            &self.query.name, match_hash, &event.event_id
                        );
                        new_output_hashes_by_match_hashes.insert(match_hash.clone(), None);
                        let (new_update_opt, mut handles) = self
                            .create_and_send_deltas_update(new_deltas, &event.event_id, match_hash)
                            .await;
                        all_handles.append(&mut handles);
                        if let Some(new_update) = new_update_opt {
                            all_updates.push(new_update);
                        }
                    }
                }
            }
        }
        //At this point the matches to possibly retract should definitely be retracted
        if !matches_to_possibly_retract.is_empty() || !new_output_hashes_by_match_hashes.is_empty()
        {
            let mut updated_output_hashes_by_match_hashes = new_output_hashes_by_match_hashes;
            for (m, o) in existing_output_hashes_by_match_hashes {
                if !matches_to_possibly_retract.contains(&m) {
                    updated_output_hashes_by_match_hashes.insert(m, o);
                }
            }
            self.store
                .update_matches(&event.event_id, updated_output_hashes_by_match_hashes);
        }

        if !matches_to_possibly_retract.is_empty() {
            debug!(
                "{} found {} matches to retract",
                &self.query.name,
                matches_to_possibly_retract.len()
            );
            let (mut retraction_updates, mut handles) = self
                .retract_matches(
                    matches_to_possibly_retract.iter().collect(),
                    &event.event_id,
                    &event.timestamp,
                )
                .await;

            all_updates.append(&mut retraction_updates);
            all_handles.append(&mut handles)
        }
        info!(
            "{} event processing took {} μs",
            &self.query.name,
            now.elapsed().as_micros()
        );
        (all_updates, all_handles)
    }

    /// Retract a vector of deltas.
    /// This is done edge by edge, by finding for each affected edge the existing deltas.
    fn retract_deltas(&mut self, deltas: Vec<DeltaAndDeltasId>) -> Vec<ReprocessInterval> {
        let retract_deltas_by_edge = get_deltas_and_deltas_id_by_edge(deltas);
        let retract_deltas_by_edge_vec: Vec<&Edge> = retract_deltas_by_edge.keys().collect();
        debug!(
            "{} retract deltas by edge {:?}",
            &self.query.name, &retract_deltas_by_edge
        );
        let existing_deltas_by_edge = self
            .store
            .get_deltas_by_edge_vec(retract_deltas_by_edge_vec);
        debug!(
            "{} existing deltas by edge {:?}",
            &self.query.name, &existing_deltas_by_edge
        );
        let updated_deltas_by_edge = self
            .store
            .remove_deltas_by_edges_and_get_updated(retract_deltas_by_edge);
        debug!(
            "{} updated deltas by edge {:?}",
            &self.query.name, &updated_deltas_by_edge
        );
        self.update_edges_and_find_intervals_to_reprocess(
            existing_deltas_by_edge,
            updated_deltas_by_edge,
        )
    }

    fn update_edges_and_find_intervals_to_reprocess(
        &mut self,
        mut existing_deltas_by_edge: BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>>,
        updated_deltas_by_edge: BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>>,
    ) -> Vec<ReprocessInterval> {
        let mut all_reprocess_intervals = vec![];
        for (e, updated_deltas_and_deltas_ids) in updated_deltas_by_edge {
            let existing_deltas_and_deltas_ids = existing_deltas_by_edge.remove(&e).unwrap();
            let existing_deltas_set =
                BTreeSet::from_iter(existing_deltas_and_deltas_ids.into_iter().map(|d| d.delta));
            let existing_deltas: Vec<Delta> = existing_deltas_set.into_iter().collect();

            let updated_deltas_set =
                BTreeSet::from_iter(updated_deltas_and_deltas_ids.into_iter().map(|d| d.delta));
            let updated_deltas: Vec<Delta> = updated_deltas_set.into_iter().collect();

            let existing_edges_with_timestamp =
                edges_from_deltas(&existing_deltas.iter().collect());
            let remaining_edges_with_timestamp =
                edges_from_deltas(&updated_deltas.iter().collect());
            self.store.update_edges(
                &remaining_edges_with_timestamp,
                &existing_edges_with_timestamp,
            );
            let mut reprocess_intervals = find_intervals_to_reprocess(
                &remaining_edges_with_timestamp,
                &existing_edges_with_timestamp,
            );
            all_reprocess_intervals.append(&mut reprocess_intervals);
        }
        all_reprocess_intervals
    }

    async fn reprocess_events_in_intervals_until_internal_update(
        &mut self,
        reprocess_intervals: Vec<ReprocessInterval>,
    ) -> (Vec<Update>, Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>, Vec<ReprocessInterval>, i32) {
        let now = Instant::now();
        let non_redundant_intervals_to_reprocess =
            find_non_redundant_intervals(reprocess_intervals);

        //These will be out default outputs
        let mut internal_updates = vec![];
        let mut all_handles = vec![];
        let mut remaining_intervals = vec![];
        let mut n_events = 0;

        'outer: for i in 0..non_redundant_intervals_to_reprocess.len() {
            let interval = non_redundant_intervals_to_reprocess.get(i).unwrap();
            let event_ids_in_interval = self
                .store
                .get_event_ids_in_interval(interval.from, interval.to);
            for j in 0..event_ids_in_interval.len() {
                let event_id = event_ids_in_interval.get(j).unwrap();
                let event_opt = self.store.get_event_by_event_id(&event_id).clone();
                if let Some(event) = event_opt {
                    let event = event.clone();
                    let (mut new_internal_updates, mut new_handles) =
                        self.process_event(&event).await;
                    n_events += 1;
                    all_handles.append(&mut new_handles);
                    //If we find the first event with an internal update, we are done
                    if new_internal_updates.len() > 0 {
                        internal_updates.append(&mut new_internal_updates);
                        if !(interval.to.is_some() && interval.to.unwrap() < (event.timestamp + 1))
                        {
                            remaining_intervals.push(ReprocessInterval {
                                from: event.timestamp,
                                to: interval.to.clone(),
                            })
                        }
                        if i + 1 < non_redundant_intervals_to_reprocess.len() {
                            for k in (i + 1)..non_redundant_intervals_to_reprocess.len() {
                                remaining_intervals.push(
                                    non_redundant_intervals_to_reprocess.get(k).unwrap().clone(),
                                );
                            }
                        }
                        break 'outer;
                    }
                }
            }
        }
        info!(
            "{} reprocess events took {} μs",
            &self.query.name,
            now.elapsed().as_micros()
        );
        (internal_updates, all_handles, remaining_intervals, n_events)
    }

    async fn process_new_match(
        &mut self,
        grouped_match: &GroupedQueryMatch,
        event: &Event,
    ) -> Option<Deltas> {
        debug!(
            "{} processing new match for event {}",
            &self.query.name, &event.event_id
        );
        let deltas_opt = self
            .caller
            .call_function(&self.query, grouped_match, event)
            .await;
        if deltas_opt.is_none() {
            debug!("{} function call had no result", &self.query.name);
        } else {
            debug!("{} function call a result", &self.query.name);
        }
        deltas_opt
    }

    async fn create_and_send_deltas_update(
        &mut self,
        deltas: Deltas,
        event_id: &str,
        match_hash: &u64,
    ) -> (
        Option<Update>,
        Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>,
    ) {
        let (my_internal_update, cascaded_query_and_update_id, handles) =
            self.router.route_deltas_update(deltas).await;
        self.store.add_new_match_updates_binding(
            event_id,
            match_hash,
            cascaded_query_and_update_id,
        );
        (my_internal_update, handles)
    }

    async fn retract_matches(
        &mut self,
        matches_hashes: Vec<&u64>,
        event_id: &str,
        event_timestamp: &u64,
    ) -> (
        Vec<Update>,
        Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>,
    ) {
        debug!(
            "{} retracting {} matches for event {}",
            &self.query.name,
            matches_hashes.len(),
            event_id
        );
        let mut all_topic_names_and_deltas_ids = vec![];
        for m in matches_hashes {
            let topic_names_and_deltas_ids_opt = self
                .store
                .pop_deltas_ids_for_event_id_and_match_hash(event_id, m);
            if let Some(mut topic_names_and_deltas_ids) = topic_names_and_deltas_ids_opt {
                all_topic_names_and_deltas_ids.append(&mut topic_names_and_deltas_ids)
            }
        }
        let (internal_updates, handles) = self
            .router
            .route_retractions(all_topic_names_and_deltas_ids, event_timestamp)
            .await;
        (internal_updates, handles)
    }

    async fn process_new_event(
        &mut self,
        event: &Event,
    ) -> (
        Vec<Update>,
        Vec<JoinHandle<Result<Response<ProcessUpdateResponse>, Status>>>,
    ) {
        self.store.add_new_event(&event);
        let (new_updates, handles) = self.process_event(&event).await;
        (new_updates, handles)
    }
}

fn get_deltas_vec_by_edge(deltas: Vec<&Delta>) -> BTreeMap<Edge, Vec<&Delta>> {
    let mut deltas_by_edge = BTreeMap::new();
    for d in deltas {
        let edge = d.to_edge();
        if !deltas_by_edge.contains_key(&edge) {
            deltas_by_edge.insert(edge, vec![d]);
        } else {
            deltas_by_edge.get_mut(&edge).unwrap().push(d);
        }
    }
    deltas_by_edge
}

fn get_deltas_and_deltas_id_by_edge(
    deltas: Vec<DeltaAndDeltasId>,
) -> BTreeMap<Edge, BTreeSet<DeltaAndDeltasId>> {
    let mut deltas_by_edge = BTreeMap::new();
    for d in deltas {
        let edge = d.delta.to_edge();
        if !deltas_by_edge.contains_key(&edge) {
            deltas_by_edge.insert(edge, BTreeSet::from([d]));
        } else {
            deltas_by_edge.get_mut(&edge).unwrap().insert(d);
        }
    }
    deltas_by_edge
}

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
use std::path::PathBuf;

use log::{debug, error};
use rusqlite::{params, Connection, Result, Row};

use mbei_core::event::{Deltas, Retractions, Update};
use mbei_core::graph::{Delta, Node};

pub struct Central {
    pub(crate) conn: Connection,
}

impl Central {
    pub fn new(db_path: PathBuf) -> Central {
        let central = Central {
            conn: Connection::open(db_path.as_path()).expect("Could not connect"),
        };
        central.create_deltas_table();
        central.create_retracted_updates_table();
        central
    }

    pub(crate) fn process_update(&self, update:Update) {
        match update {
                Update::Deltas(deltas) => {
                    debug!("Received delta update with deltas id {}, event_id {}", &deltas.deltas_id, &deltas.origin_id);
                    if !self.is_update_retracted(&deltas.deltas_id) {
                        self.insert_deltas(&deltas);
                    } else {
                        debug!("Update was retracted, will do nothing");
                    }
                }
                Update::Retractions(retractions) => {
                    debug!("Received retractions");
                    self.retract_deltas(&retractions);
                }
                _ => {error!("Should never happen"); }
            };
    }

    fn create_deltas_table(&self) {
        let query = "CREATE TABLE IF NOT EXISTS deltas
                        (deltas_id STRING,
                        ts INT,
                        delta_type STRING,
                        src_name STRING,
                        src_nodetype STRING,
                        src_nodeclass STRING,
                        src_value BYTES,
                        trg_name STRING,
                        trg_nodetype STRING,
                        trg_nodeclass STRING,
                        trg_value BYTES,
                        edge_type STRING);";
        let index_query =
            "CREATE INDEX IF NOT EXISTS deltas_deltas_id_index ON deltas (deltas_id);";
        self.conn.execute(query, []).expect("Could not execute");
        self.conn
            .execute(index_query, [])
            .expect("Could not execute");
    }

    fn create_retracted_updates_table(&self) {
        let query = "CREATE TABLE IF NOT EXISTS retracted_updates
                (deltas_id STRING PRIMARY KEY);";
        self.conn.execute(query, []).expect("Could not execute");
    }


    fn is_update_retracted(&self, deltas_id:&str) -> bool {
        let query = "SELECT 1 FROM retracted_updates WHERE deltas_id=:updateid";
        let mut stmt = self.conn.prepare(query).expect("Could not prepare");

        let rows = stmt
            .query_map(&[(":updateid", &deltas_id)], |row| -> Result<u8> {
                Ok(row.get(0).expect("Could not find index 0"))
            })
            .expect("Could not map result");
        for _ in rows {
            return true;
        }
        return false;
    }

    fn insert_deltas(&self, deltas: &Deltas) {
        let query = "INSERT INTO deltas
                (deltas_id, src_name, src_nodetype, src_nodeclass, src_value,
                trg_name, trg_nodetype, trg_nodeclass, trg_value, edge_type, delta_type, ts)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)";
        for delta in &deltas.deltas {
            debug!("Inserting delta {:?}", delta);
            self.conn
                .execute(
                    query,
                    params![
                        &deltas.deltas_id,
                        &delta.src.instance_node_name.as_ref().unwrap(),
                        &delta.src.node_type.as_ref().unwrap(),
                        &delta.src.node_class.to_string(),
                        &delta.src.value_bytes,
                        &delta.trg.instance_node_name.as_ref().unwrap(),
                        &delta.trg.node_type.as_ref().unwrap(),
                        &delta.trg.node_class.to_string(),
                        &delta.trg.value_bytes,
                        &delta.edge_type.to_string(),
                        &delta.delta_type.to_string(),
                        &delta.timestamp.to_string()
                    ],
                )
                .expect("Could not execute query");
        }
    }

    fn retract_deltas(&self, retractions: &Retractions) {
        debug!("Retracting: {:?}", &retractions.deltas_ids);
        let delete_query = "DELETE FROM deltas WHERE deltas_id = (?1);";
        let insert_query = "INSERT OR IGNORE INTO retracted_updates (deltas_id) VALUES (?1)";
        let deltas_ids_set = BTreeSet::from_iter(retractions.deltas_ids.iter());
        for deltas_id in deltas_ids_set {
            self.conn
                .execute(delete_query, params![deltas_id])
                .expect("Could not execute query");
            self.conn
                .execute(insert_query, params![deltas_id])
                .expect("Could not execute query");
        }
    }

    pub fn get_all_deltas(&self) -> Vec<Delta> {
        let query = "SELECT src_name, src_nodetype, src_nodeclass, src_value,
                trg_name, trg_nodetype, trg_nodeclass, trg_value, edge_type, ts, delta_type FROM deltas";
        let mut stmt = self.conn.prepare(query).expect("Could not prepare");
        let rows = stmt
            .query_map([], |row| delta_from_tuple(row))
            .expect("Could not map result");
        let mut deltas = vec![];
        for r in rows {
            deltas.push(r.expect("Error mapping row"));
        }
        deltas
    }
}

fn delta_from_tuple(row: &Row) -> Result<Delta> {
    let src = Node {
        query_node_name: None,
        instance_node_name: row.get(0).unwrap(),
        node_type: row.get(1).unwrap(),
        node_class: row.get(2).unwrap(),
        value_bytes: row.get(3).unwrap(),
    };
    let trg = Node {
        query_node_name: None,
        instance_node_name: row.get(4).unwrap(),
        node_type: row.get(5).unwrap(),
        node_class: row.get(6).unwrap(),
        value_bytes: row.get(7).unwrap(),
    };
    Ok(Delta {
        src,
        trg,
        edge_type: row.get(8).unwrap(),
        timestamp: row.get(9).unwrap(),
        delta_type: row.get(10).unwrap(),
    })
}

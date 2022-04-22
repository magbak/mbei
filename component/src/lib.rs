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
use std::time::Duration;

use crate::component::Component;
use crate::server::ComponentServer;
use log::{debug, info};
use mbei_core::query::Query;
use mbei_grpc::process_update_client::await_deliveries;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tonic::Status;
use mbei_grpc::process_update::ProcessUpdateResponse;

pub mod caller;
mod component;
mod intervals;
pub mod router;
mod server;
pub mod store;

type JoinHandleType = JoinHandle<Result<tonic::Response<ProcessUpdateResponse>, Status>>;

pub async fn start_component_servers(
    queries: Vec<Query>,
    my_query_names: Vec<String>,
    application_grpc_url: String,
    grpc_port: u16,
    query_url_map: BTreeMap<String, String>,
    max_elapsed_time: Option<Duration>,
    use_central: bool,
) {
    let mut all_queries_by_name = BTreeMap::new();
    for query in queries {
        all_queries_by_name.insert(query.name.clone(), query);
    }
    info!("Starting components: {:?}", &my_query_names);

    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let deliveries_handle;
    deliveries_handle = tokio::spawn(await_deliveries(receiver));
    {
        //This block is important, since it moves and then drops all senders, causing the recv of the receiver to return None and deliveries handle to finish.
        let mut component_server_handles = vec![];
        let mut i = 0;
        for query_name in &my_query_names {
            let use_grpc_port = grpc_port + i;
            let component_server_handle = tokio::spawn(create_owned_component_server_and_run(
                query_name.clone(),
                all_queries_by_name.clone(),
                query_url_map.clone(),
                use_central,
                application_grpc_url.clone(),
                use_grpc_port,
                max_elapsed_time.clone(),
                sender.clone(),
            ));
            component_server_handles.push(component_server_handle);
            i += 1;
        }
        for h in component_server_handles {
            h.await.expect("Problem in server");
        }
    }
    drop(sender);
    debug!(
        "Servers for {:?} was shut down, awaiting deliveries",
        &my_query_names
    );
    deliveries_handle.await.expect("Problem with deliveries");
    debug!(
        "Finished awaiting deliveries, components {:?} finished",
        &my_query_names
    )
}

async fn create_owned_component_server_and_run(
    my_query_name: String,
    all_queries_by_name: BTreeMap<String, Query>,
    query_url_map: BTreeMap<String, String>,
    use_central: bool,
    application_grpc_url: String,
    grpc_port: u16,
    max_elapsed_time: Option<Duration>,
    sender: UnboundedSender<JoinHandleType>,
) {
    let component = Component::new(
        my_query_name.clone(),
        all_queries_by_name,
        application_grpc_url,
        query_url_map,
        use_central,
    );

    let mut component_server =
        ComponentServer::new(my_query_name.clone(), component, grpc_port, sender);
    component_server.run(max_elapsed_time).await;
}

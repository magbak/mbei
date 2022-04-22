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
use std::str::FromStr;
use std::time::Duration;
use backoff::{ExponentialBackoffBuilder};

use log::{debug};
use tonic::transport::Endpoint;
use uuid::Uuid;

use mbei_core::event::{Deltas, Event};
use mbei_core::query::{GroupedQueryMatch, Query};

use mbei_grpc::application_component::call_application_client::CallApplicationClient;
use mbei_grpc::application_component_mapping::{create_application_request, delta_vec_from_response};

pub struct Caller {
    endpoint: Endpoint,
    client: Option<CallApplicationClient<tonic::transport::Channel>>,
}

impl Caller {
    pub(crate) fn new(grpc_url: String) -> Caller {
        let endpoint = Endpoint::from_str(&grpc_url).expect("Invalid url");
        Caller {
            endpoint,
            client: None,
        }
    }

    pub(crate) async fn start(&mut self, max_elapsed_time: Option<Duration>) {
        debug!("Starting caller");
        let op = || async {
            let client = CallApplicationClient::connect(self.endpoint.clone()).await?;
            Ok(client)
        };

        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(max_elapsed_time)
            .with_max_interval(Duration::from_secs(10))
            .build();
        self.client = Some(
            backoff::future::retry(backoff, op)
                .await
                .expect("Could not connect to backend grpc service in maximum allotted time"),
        );
        debug!("Caller started");
    }

    pub(crate) async fn call_function(
        &mut self,
        query: &Query,
        grouped_match: &GroupedQueryMatch,
        event: &Event,
    ) -> Option<Deltas> {
        assert!(self.client.is_some());
        let request = create_application_request(
            query.name.clone(),
            query.application.clone(),
            grouped_match,
            &query.graph,
            event,
        );
        let response = self.client.as_mut().unwrap().send(request).await.expect("Error sending").into_inner();
        let delta_vec = delta_vec_from_response(response);
        if !delta_vec.is_empty() {
            let deltas = mbei_core::event::Deltas{
                deltas_id: Uuid::new_v4().to_hyphenated().to_string(),
                origin_id: event.event_id.clone(),
                origin_timestamp: event.timestamp,
                deltas: BTreeSet::from_iter(delta_vec.into_iter()),
            };
            return Some(deltas);
        }
        None
    }
}

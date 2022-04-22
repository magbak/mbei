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

use std::net::SocketAddr;
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use log::{info};
use tokio::runtime::Runtime;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use crate::common::application_component::{ApplicationRequest, ApplicationResponse};
use crate::common::application_component::call_application_server::{CallApplication, CallApplicationServer};
use crate::conveyor::ConveyorApplication;
use crate::crane::CraneApplication;
use crate::detector::DetectorApplication;
use crate::stamp::StampApplication;

pub mod conveyor;
pub mod crane;
pub mod detector;
pub mod stamp;
mod common;


pub async fn start_serving(port: u16) {
    info!("Starting server at port {}", &port);
    let address_string = "[::]:".to_owned() + &port.to_string();
    let address: SocketAddr = address_string.parse().expect("Error parsing server address");
    let application_component_service = ApplicationComponentService {
        crane: CraneApplication::new(),
        stamp: StampApplication::new(),
        conveyor: ConveyorApplication::new(),
        detector: DetectorApplication::new(),
    };
    let svc = CallApplicationServer::new(application_component_service);
    Server::builder()
        .add_service(svc)
        .serve(address)
        .await
        .expect("Serving failed");
}

struct ApplicationComponentService {
    crane: CraneApplication,
    stamp: StampApplication,
    conveyor: ConveyorApplication,
    detector: DetectorApplication,
}

#[tonic::async_trait]
impl CallApplication for ApplicationComponentService {
    async fn send(
        &self,
        request: Request<ApplicationRequest>,
    ) -> Result<Response<ApplicationResponse>, Status> {
        let request = request.get_ref();
        match &request.application_name {
            name if name == "pickdrop" => self.crane.crane_function(request),
            name if name == "stamp" => self.stamp.stamp_function(request),
            name if name == "conveyor" => self.conveyor.conveyor_function(request),
            name if name == "detector" => self.detector.detector_function(request),
            name => Err(Status::invalid_argument(
                "Bad application name: ".to_string() + &name.clone(),
            )),
        }
    }
}


pub fn create_tonic_server(port:u16) -> JoinHandle<()> {
    let rt = Runtime::new().expect("Could not create runtime");
    let handle = thread::spawn(move || {
        rt.block_on(async {
            let handle = tokio::spawn(start_serving(port));
            handle.await.expect("Panic in grpc server");
        });
    });
    sleep(Duration::from_secs(1));
    handle
}

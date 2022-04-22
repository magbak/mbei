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

use std::path::PathBuf;
use log::debug;

use tokio::runtime::Runtime;

pub use crate::central::Central;
use crate::server::CentralServer;

mod central;
mod server;

pub fn start_central(sqlite_path: PathBuf, grpc_port: u16) {
    let central = Central::new(sqlite_path.clone());
    let central_server = CentralServer::new(grpc_port, central);
    let rt = Runtime::new().expect("Could not create runtime");
    rt.block_on(central_server.run());
    debug!("Closing database");
    central_server.close();
    debug!("Database closed");
}


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

use structopt::StructOpt;

use mbei_central::start_central;

#[derive(StructOpt)]
pub struct Cli {
    #[structopt(short = "-s", long = "--sqlite-path", parse(from_os_str))]
    pub sqlite_path: std::path::PathBuf,

    #[structopt(short = "-p", long = "--port")]
    pub port: u16,
}

fn main() {
    env_logger::init();
    let cli: Cli = Cli::from_args();
    start_central(cli.sqlite_path, cli.port);

}

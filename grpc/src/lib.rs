pub mod delta {
    tonic::include_proto!("delta");
}

pub mod event {
    tonic::include_proto!("event");
}

pub mod application_component {
    tonic::include_proto!("application_component");
}

pub mod process_update {
    tonic::include_proto!("process_update");
}

mod delta_mapping;
mod event_mapping;
pub mod application_component_mapping;
pub mod process_update_mapping;
pub mod process_update_client;
pub mod process_update_server;

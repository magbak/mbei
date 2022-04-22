use std::collections::BTreeSet;
use crate::delta_mapping::{from_proto_delta, to_proto_delta};
use crate::event_mapping::{from_proto_event, to_proto_event};
use crate::process_update::process_update_request::Update;
use crate::process_update::{ProcessUpdateRequest, Stop};

pub fn update_from_request(request: &ProcessUpdateRequest) -> mbei_core::event::Update {
    let update = request.update.as_ref().unwrap();
    match update {
        Update::Stop(_) => {
            mbei_core::event::Update::Stop
        }
        Update::Event(e) => {
            mbei_core::event::Update::Event(from_proto_event(e.clone()))
        }
        Update::Deltas(ds) => {
            mbei_core::event::Update::Deltas(from_proto_deltas(ds.clone()))
        }
        Update::Retractions(rs) => {
            mbei_core::event::Update::Retractions(from_proto_retractions(rs.clone()))
        }
    }
}

pub fn request_from_update(update: &mbei_core::event::Update) -> ProcessUpdateRequest {
    ProcessUpdateRequest { update: Some(to_proto_update(update)) }
}

fn to_proto_update(update: &mbei_core::event::Update) -> Update {
    match update {
        mbei_core::event::Update::Stop => {Update::Stop(Stop {})}
        mbei_core::event::Update::Event(e) => {Update::Event(to_proto_event(e))}
        mbei_core::event::Update::Deltas(ds) => {Update::Deltas(to_proto_deltas(ds))}
        mbei_core::event::Update::Retractions(rs) => {Update::Retractions(to_proto_retractions(rs))}
    }
}

fn from_proto_deltas(ds:crate::process_update::Deltas) -> mbei_core::event::Deltas {
    mbei_core::event::Deltas {
        deltas_id: ds.deltas_id,
        origin_id: ds.origin_id,
        origin_timestamp: ds.origin_timestamp,
        deltas: BTreeSet::from_iter(ds.deltas.into_iter().map(from_proto_delta)),
    }
}

fn to_proto_deltas(ds:&mbei_core::event::Deltas) -> crate::process_update::Deltas {
    crate::process_update::Deltas {
        deltas_id: ds.deltas_id.clone(),
        origin_id: ds.origin_id.clone(),
        origin_timestamp: ds.origin_timestamp,
        deltas: ds.deltas.iter().map(to_proto_delta).collect()
    }
}

fn from_proto_retractions(rs:crate::process_update::Retractions) -> mbei_core::event::Retractions {
    mbei_core::event::Retractions {
        retraction_id: rs.retraction_id,
        timestamp: rs.origin_timestamp,
        deltas_ids: rs.delta_ids
    }
}

fn to_proto_retractions(rs : &mbei_core::event::Retractions) -> crate::process_update::Retractions {
    crate::process_update::Retractions {
        retraction_id: rs.retraction_id.clone(),
        origin_timestamp: rs.timestamp,
        delta_ids: rs.deltas_ids.clone()
    }
}
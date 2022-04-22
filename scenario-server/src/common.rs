use application_component::{ApplicationResponse, QueryNode};
use delta::{InstanceNode};
use tonic::{Response, Status};

pub mod event {
    tonic::include_proto!("event");
}

pub mod delta {
    tonic::include_proto!("delta");
}

pub mod application_component {
    tonic::include_proto!("application_component");
}

pub(crate) fn empty_response() -> Result<Response<ApplicationResponse>, Status> {
    Ok(Response::new(ApplicationResponse { deltas: vec![] }))
}

pub(crate) fn as_instance_node(query_node: &QueryNode) -> InstanceNode {
    assert!(query_node.instance_node_id.is_some());
    assert!(query_node.node_type.is_some());

    InstanceNode {
        instance_node_id: query_node.instance_node_id.as_ref().unwrap().s.clone(),
        node_type: query_node.node_type.as_ref().unwrap().s.clone(),
        node_class: query_node.node_class,
        value: None,
    }
}
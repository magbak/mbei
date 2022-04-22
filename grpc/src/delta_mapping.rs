pub(crate) fn from_proto_delta_vec(deltas: Vec<crate::delta::Delta>) -> Vec<mbei_core::graph::Delta> {
    deltas.into_iter().map(|pd| from_proto_delta(pd)).collect()
}

pub(crate) fn from_proto_delta(proto_delta: crate::delta::Delta) -> mbei_core::graph::Delta {
    mbei_core::graph::Delta {
        src: from_instance_node(proto_delta.src.unwrap()),
        trg: from_instance_node(proto_delta.trg.unwrap()),
        edge_type: proto_delta.edge_type,
        timestamp: proto_delta.timestamp,
        delta_type: from_proto_delta_type(proto_delta.delta_type),
    }
}

pub(crate) fn to_proto_delta(delta:&mbei_core::graph::Delta) -> crate::delta::Delta {
    crate::delta::Delta {
        src: Some(to_instance_node(&delta.src)),
        trg: Some(to_instance_node(&delta.trg)),
        edge_type: delta.edge_type.clone(),
        timestamp: delta.timestamp,
        delta_type: to_proto_delta_type(&delta.delta_type)
    }
}

pub(crate) fn from_proto_delta_type(proto_delta_type: i32) -> mbei_core::graph::DeltaType {
    match proto_delta_type.try_into() {
        Ok(delta_type) => delta_type,
        _ => panic!("Could not convert to delta type"),
    }
}

pub(crate) fn to_proto_delta_type(delta_type:&mbei_core::graph::DeltaType) -> i32 {
    delta_type.clone() as i32
}

pub fn from_instance_node(instance_node: crate::delta::InstanceNode) -> mbei_core::graph::Node {
    mbei_core::graph::Node {
        query_node_name: None,
        instance_node_name: Some(instance_node.instance_node_id),
        node_type: Some(instance_node.node_type),
        node_class: from_proto_node_class(instance_node.node_class),
        value_bytes: from_optional_bytes(instance_node.value),
    }
}

fn from_optional_bytes(optional_bytes_opt: Option<crate::delta::OptionalBytes>) -> Option<Vec<u8>> {
    match optional_bytes_opt {
        Some(optional_bytes) => Some(optional_bytes.b),
        None => None,
    }
}

pub(crate) fn from_proto_node_class(proto_node_class: i32) -> mbei_core::graph::NodeClass {
    match proto_node_class.try_into() {
        Ok(delta_type) => delta_type,
        _ => panic!("Could not convert to delta type"),
    }
}

pub(crate) fn to_instance_node(node: &mbei_core::graph::Node) -> crate::delta::InstanceNode {
    let value = match &node.value_bytes {
        Some(value) => Some(crate::delta::OptionalBytes { b: value.clone() }),
        None => None,
    };

    crate::delta::InstanceNode {
        instance_node_id: node.instance_node_name.as_ref().unwrap().clone(),
        node_type: node.node_type.as_ref().unwrap().clone(),
        node_class: to_proto_node_class(&node.node_class),
        value,
    }
}


pub(crate) fn to_proto_node_class(node_class: &mbei_core::graph::NodeClass) -> i32 {
    node_class.clone() as i32
}

use crate::delta_mapping::{from_proto_delta_vec, to_instance_node, to_proto_node_class};
use crate::event_mapping::to_proto_event;

pub fn create_application_request(
    query_name: String,
    application_name: String,
    grouped_match: &mbei_core::query::GroupedQueryMatch,
    query_graph: &mbei_core::graph::Graph,
    event: &mbei_core::event::Event,
) -> crate::application_component::ApplicationRequest {
    crate::application_component::ApplicationRequest {
        query_name,
        application_name,
        matches: to_query_matches(grouped_match),
        query_graph: to_query_graph(&query_graph),
        event: Some(to_proto_event(event)),
    }
}

pub fn delta_vec_from_response(application_response:crate::application_component::ApplicationResponse) -> Vec<mbei_core::graph::Delta> {
    from_proto_delta_vec(application_response.deltas)
}

fn to_query_matches(
    grouped_query_match: &mbei_core::query::GroupedQueryMatch,
) -> Vec<crate::application_component::Match> {
    grouped_query_match
        .grouped_matches
        .iter()
        .map(|m| to_query_match(m))
        .collect()
}

fn to_query_match(m: &mbei_core::query::QueryMatch) -> crate::application_component::Match {
    let mut tuples = vec![];
    for (src_edge, trg_edge_opt) in &m.homomorphism {
        if let Some(trg_edge) = trg_edge_opt {
            tuples.push(to_match_tuple(src_edge, trg_edge))
        }
    }
    crate::application_component::Match { tuples }
}

fn to_match_tuple(
    src_edge: &mbei_core::graph::Edge,
    trg_edge: &mbei_core::graph::Edge,
) -> crate::application_component::MatchTuple {
    crate::application_component::MatchTuple {
        src: Some(to_query_edge(src_edge)),
        trg: Some(to_instance_edge(trg_edge)),
    }
}

fn to_query_graph(graph: &mbei_core::graph::Graph) -> Vec<crate::application_component::QueryEdge> {
    graph.edges.iter().map(|e| to_query_edge(e)).collect()
}

fn to_query_edge(edge: &mbei_core::graph::Edge) -> crate::application_component::QueryEdge {
    crate::application_component::QueryEdge {
        src: Some(to_query_node(&edge.src)),
        trg: Some(to_query_node(&edge.trg)),
        edge_type: edge.edge_type.clone(),
    }
}

fn to_instance_edge(edge: &mbei_core::graph::Edge) -> crate::application_component::InstanceEdge {
    crate::application_component::InstanceEdge {
        src: Some(to_instance_node(&edge.src)),
        trg: Some(to_instance_node(&edge.trg)),
        from_timestamp: edge.from_timestamp.unwrap(),
        edge_type: edge.edge_type.clone(),
    }
}

fn to_query_node(node: &mbei_core::graph::Node) -> crate::application_component::QueryNode {
    let instance_node_id = match &node.instance_node_name {
        Some(node_name) => Some(crate::application_component::OptionalString {
            s: node_name.clone(),
        }),
        None => None,
    };
    let node_type = match &node.node_type {
        Some(node_type) => Some(crate::application_component::OptionalString {
            s: node_type.clone(),
        }),
        None => None,
    };

    crate::application_component::QueryNode {
        query_node_name: node.query_node_name.as_ref().unwrap().clone(),
        instance_node_id,
        node_type,
        node_class: to_proto_node_class(&node.node_class),
    }
}

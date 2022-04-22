use mbei_component::store::Store;
use mbei_core::event::Event;
use rstest::{fixture, rstest};
use serial_test::serial;
#[cfg(test)]
use std::collections::BTreeSet;
use mbei_core::graph::{Edge, Node};

#[fixture]
#[once]
pub fn start_logging() {
    env_logger::init();
}

#[fixture]
pub fn teststore() -> Store {
    Store::new()
}

#[fixture]
pub fn abc1c2d_events() -> Vec<Event> {
    let a = Event {
        event_id: "A".to_string(),
        timestamp: 1,
        node_id: "A_nodeid".to_string(),
        payload: vec![],
    };
    let b = Event {
        event_id: "B".to_string(),
        timestamp: 3,
        node_id: "B_nodeid".to_string(),
        payload: vec![],
    };
    let c1 = Event {
        event_id: "C2".to_string(),
        timestamp: 915,
        node_id: "C1_nodeid".to_string(),
        payload: vec![],
    };
    let c2 = Event {
        event_id: "C1".to_string(),
        timestamp: 915,
        node_id: "C2_nodeid".to_string(),
        payload: vec![],
    };

    let d = Event {
        event_id: "D".to_string(),
        timestamp: 1000,
        node_id: "C_nodeid".to_string(),
        payload: vec![],
    };
    vec![a, b, c1, c2, d]
}

#[fixture]
pub fn abc_events() -> Vec<Event> {
    let a = Event {
        event_id: "A".to_string(),
        timestamp: 226,
        node_id: "A_nodeid".to_string(),
        payload: vec![],
    };
    let b = Event {
        event_id: "B".to_string(),
        timestamp: 235,
        node_id: "B_nodeid".to_string(),
        payload: vec![],
    };
    let c = Event {
        event_id: "C".to_string(),
        timestamp: 274,
        node_id: "C_nodeid".to_string(),
        payload: vec![],
    };
    vec![a, b, c]
}

#[rstest]
#[serial]
fn test_event_iterator_after_first(mut teststore: Store, abc1c2d_events: Vec<Event>) {
    for event in &abc1c2d_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(2, None);
    assert_eq!(
        BTreeSet::from_iter(result.into_iter()),
        BTreeSet::from([
            "B".to_string(),
            "C2".to_string(),
            "C1".to_string(),
            "D".to_string()
        ])
    );
}

#[rstest]
#[serial]
fn test_event_iterator_from_after_end(mut teststore: Store, abc1c2d_events: Vec<Event>) {
    for event in &abc1c2d_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(1167, None);
    assert!(result.is_empty());
}

#[rstest]
#[serial]
fn test_event_iterator_from_before_start(mut teststore: Store, abc1c2d_events: Vec<Event>) {
    for event in &abc1c2d_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(0, None);
    assert_eq!(
        BTreeSet::from_iter(result.into_iter()),
        BTreeSet::from([
            "A".to_string(),
            "B".to_string(),
            "C2".to_string(),
            "C1".to_string(),
            "D".to_string()
        ])
    );
}

#[rstest]
#[serial]
fn test_event_iterator_from_before_stops_inside(mut teststore: Store, abc1c2d_events: Vec<Event>) {
    for event in &abc1c2d_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(0, Some(3));
    assert_eq!(
        BTreeSet::from_iter(result.into_iter()),
        BTreeSet::from(["A".to_string(), "B".to_string()])
    );
}

#[rstest]
#[serial]
fn test_event_iterator_from_inside_stops_inside(mut teststore: Store, abc1c2d_events: Vec<Event>) {
    for event in &abc1c2d_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(3, Some(915));
    assert_eq!(
        BTreeSet::from_iter(result.into_iter()),
        BTreeSet::from(["B".to_string(), "C1".to_string(), "C2".to_string()])
    );
}

#[rstest]
#[serial]
fn test_finds_both_events(mut teststore: Store, abc_events: Vec<Event>) {
    for event in &abc_events {
        teststore.add_new_event(&event);
    }

    let result = teststore.get_event_ids_in_interval(232, None);
    assert_eq!(
        BTreeSet::from_iter(result.into_iter()),
        BTreeSet::from(["B".to_string(), "C".to_string()])
    );
}

#[rstest]
#[serial]
fn test_node_indexed_edges(mut teststore:Store) {
    let n1 = Node::material_instance_node("MyBarrel0", "Barrel");
    let n2 = Node::property_instance_node("MyBarrel0_barrel_type", "BarrelMaterialType", vec![]);
    let n3 = Node::object_instance_node("Somewhere", "SomewhereType");
    let edge1 = Edge {
        src: n1.clone(),
        trg: n2,
        edge_type: "HasMaterialType".to_string(),
        from_timestamp: Some(4),
        to_timestamp: None
    };
    let edge2 = Edge {
        src: n1,
        trg: n3,
        edge_type: "At".to_string(),
        from_timestamp: Some(3),
        to_timestamp: None
    };

    teststore.update_edges(&vec![edge1, edge2], &vec![]);
    let edges = teststore.get_edges_at_timestamp(5);
    assert_eq!(edges.len(), 2);
}
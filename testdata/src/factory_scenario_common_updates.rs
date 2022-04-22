use std::collections::{BTreeMap, BTreeSet};
use bincode::config::Configuration;

use log::info;
use uuid::Uuid;

use crate::producer::{create_single_delta_update, TopicNameAndUpdate};
use mbei_core::event::{Event, Update};
use mbei_core::graph::{Delta, DeltaType, Node};
use mbei_core::query::Query;
use mbei_scenario_server::crane::{CraneEvent, CraneEventType};
use mbei_scenario_server::stamp::{create_stamp_node, StampEvent};

pub(crate) fn create_pickdrop_belongs_to_crane_map(
    queries: &Vec<Query>,
) -> BTreeMap<Node, Node> {
    let mut pickdrop_belongs_to_crane = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.src.node_type.as_ref().unwrap() == "Crane" && e.edge_type == "HasEvent" {
                pickdrop_belongs_to_crane.insert(trg_clone, src_clone);
            }
        }
    }
    pickdrop_belongs_to_crane
}

pub(crate) fn create_objects_surrounding_crane_map(
    queries: &Vec<Query>,
) -> BTreeMap<Node, Vec<Node>> {
    let has_object_map = create_has_object_map(queries);
    let mut objects_surrounding_crane_map = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.edge_type == "HasObjectAtPosition" {
                let new_trg = has_object_map.get(&trg_clone).unwrap().clone();
                if !objects_surrounding_crane_map.contains_key(&src_clone) {
                    objects_surrounding_crane_map.insert(src_clone, vec![new_trg]);
                } else {
                    objects_surrounding_crane_map
                        .get_mut(&src_clone)
                        .unwrap()
                        .push(new_trg);
                }
            }
        }
    }
    objects_surrounding_crane_map
}

pub(crate) fn create_stamp_assembly_of_map(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut stamp_assembly_of_map = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            let mut src_clone = e.src.clone();
            src_clone.forget_query_node_name();
            let mut trg_clone = e.trg.clone();
            trg_clone.forget_query_node_name();
            if e.src.node_type.as_ref().unwrap() == "StampAssembly"
                && e.trg.node_type.as_ref().unwrap() == "Stamp"
            {
                stamp_assembly_of_map.insert(trg_clone, src_clone);
            }
        }
    }
    stamp_assembly_of_map
}

pub(crate) fn create_topic_names_map(
    at_candidates: &Vec<Node>,
    queries: &Vec<Query>,
) -> BTreeMap<Node, Vec<String>> {
    let mut topic_names_map = BTreeMap::new();
    let mut topic_names;
    for c in at_candidates {
        topic_names = BTreeSet::new();
        for q in queries {
            for i in &q.input_nodes {
                if c.instance_node_name == i.instance_node_name {
                    topic_names.insert(q.name.clone());
                }
            }
            for e in &q.graph.edges {
                if c.instance_node_name == e.src.instance_node_name
                    || c.instance_node_name == e.trg.instance_node_name
                {
                    topic_names.insert(q.name.clone());
                }
            }
        }
        topic_names_map.insert(c.clone(), topic_names);
    }
    let tuples: Vec<(Node, Vec<String>)> = topic_names_map
        .into_iter()
        .map(|(c, s)| (c, s.into_iter().collect()))
        .collect();
    let topic_names_map = BTreeMap::from_iter(tuples.into_iter());
    topic_names_map
}

pub(crate) fn create_has_object_map(queries: &Vec<Query>) -> BTreeMap<Node, Node> {
    let mut has_object = BTreeMap::new();
    for q in queries {
        for e in &q.graph.edges {
            if e.edge_type == "HasObject" {
                let mut src_clone = e.src.clone();
                src_clone.forget_query_node_name();
                let mut trg_clone = e.trg.clone();
                trg_clone.forget_query_node_name();
                has_object.insert(src_clone, trg_clone);
            }
        }
    }
    has_object
}

fn create_crane_event(
    crane_pickdrop: Node,
    object: Node,
    crane_event_type: CraneEventType,
    timestamp: u64,
    topic: String,
    config: Configuration
) -> TopicNameAndUpdate {
    info!(
        "Crane event at {}: {}, {:?}, {}",
        &timestamp,
        &crane_pickdrop.instance_node_name.as_ref().unwrap(),
        &crane_event_type,
        &object.instance_node_name.as_ref().unwrap()
    );
    let crane_event = CraneEvent {
        instance_node_id: object.instance_node_name.as_ref().unwrap().clone(),
        crane_event_type,
    };
    let event = Event {
        event_id: Uuid::new_v4().to_hyphenated().to_string(),
        timestamp,
        node_id: crane_pickdrop.instance_node_name.as_ref().unwrap().clone(),
        payload: bincode::encode_to_vec(&crane_event, config).expect("Encoding error"),
    };
    TopicNameAndUpdate::new(topic, Update::Event(event))
}

fn create_stamp_event(
    stamp: Node,
    timestamp: u64,
    topic: String,
    config: Configuration
) -> (Vec<u8>, TopicNameAndUpdate) {
    info!(
        "Stamp event at {}: {}",
        &timestamp,
        &stamp.instance_node_name.as_ref().unwrap()
    );
    let stamp_event = StampEvent {
        stamp_data: Uuid::new_v4().to_hyphenated().to_string(),
    };
    let payload = bincode::encode_to_vec(&stamp_event, config).expect("Encoding error");
    let event = Event {
        event_id: Uuid::new_v4().to_hyphenated().to_string(),
        timestamp,
        node_id: stamp.instance_node_name.as_ref().unwrap().clone(),
        payload: payload.clone(),
    };
    (payload, TopicNameAndUpdate::new(topic, Update::Event(event)))
}

pub(crate) fn create_new_barrel(
    node: Node,
    barrel: Node,
    timestamp: u64,
    topic_names: Vec<String>,
    use_central: bool,
) -> Vec<TopicNameAndUpdate> {
    info!(
        "Push new barrel at {}: {} onto {}",
        &timestamp,
        &barrel.instance_node_name.as_ref().unwrap(),
        &node.instance_node_name.as_ref().unwrap()
    );
    let update = create_single_delta_update(barrel, node, "At", timestamp, DeltaType::Addition);
    let mut topics_and_updates = vec![];
    if use_central {
        topics_and_updates.push(TopicNameAndUpdate::new("central".to_string(), update.clone()));
    }
    for t in topic_names {
        topics_and_updates.push(TopicNameAndUpdate::new(t, update.clone()));
    }
    topics_and_updates
}

pub(crate) fn create_remove_barrel_updates(
    node: Node,
    barrel: Node,
    timestamp: u64,
    topic_names: Vec<String>,
    use_central: bool,
) -> Vec<TopicNameAndUpdate> {
    info!(
        "Remove barrel at {}: {} from {}",
        &timestamp,
        &barrel.instance_node_name.as_ref().unwrap(),
        &node.instance_node_name.as_ref().unwrap()
    );
    let update = create_single_delta_update(barrel, node, "At", timestamp, DeltaType::Removal);
    let mut topics_and_updates = vec![];
    if use_central {
        topics_and_updates.push(TopicNameAndUpdate::new("central".to_string(), update.clone()));
    }
    for t in topic_names {
        topics_and_updates.push(TopicNameAndUpdate::new(t, update.clone()));
    }
    topics_and_updates
}


pub fn new_barrel() -> Node {
    Node::material_instance_node(
        &("MyBarrel-".to_owned() + &Uuid::new_v4().to_hyphenated().to_string()),
        "Barrel",
    )
}

pub(crate) fn create_stamp_message(
    stamp: Node,
    stamp_assembly: Node,
    state: &BTreeMap<Node, Option<Node>>,
    topic_names_map: &BTreeMap<Node, Vec<String>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    config: Configuration
) -> TopicNameAndUpdate {
    let barrel = state.get(&stamp_assembly).unwrap().as_ref().unwrap();
    let topic_name = topic_names_map.get(&stamp).unwrap().get(0).unwrap().clone();
    let (payload, topic_name_and_update) =
        create_stamp_event(stamp, current_timestamp + 1, topic_name, config);
    //This is quite a dirty fix..
    let trg_node_wrong_classtype = create_stamp_node(
        barrel.instance_node_name.as_ref().unwrap().clone(),
        current_timestamp + 1,
        payload.clone(),
    );
    let trg_node = Node::property_instance_node(
        &trg_node_wrong_classtype.instance_node_id,
        &trg_node_wrong_classtype.node_type,
        payload,
    );

    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: trg_node,
        edge_type: "HasStampData".to_string(),
        timestamp: current_timestamp + 1,
        delta_type: DeltaType::Addition,
    });
    topic_name_and_update
}

pub(crate) fn create_pickup_message(
    barrel: Node,
    crane: Node,
    crane_event: Node,
    from_node: Node,
    state: &mut BTreeMap<Node, Option<Node>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_name:String,
    config: Configuration
) -> TopicNameAndUpdate {
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: from_node.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp,
        delta_type: DeltaType::Removal,
    });
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: crane.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp + 1,
        delta_type: DeltaType::Addition,
    });

    let cloned_barrel = Some(barrel.clone());
    state.insert(crane_event.clone(), cloned_barrel);
    state.insert(from_node.clone(), None);

    create_crane_event(
        crane_event.clone(),
        from_node.clone(),
        CraneEventType::PickUp,
        current_timestamp,
        topic_name,
        config
    )
}

pub(crate) fn create_drop_message(
    barrel: Node,
    crane: Node,
    crane_event: Node,
    to_node: Node,
    state: &mut BTreeMap<Node, Option<Node>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_name:String,
    config: Configuration,
) -> TopicNameAndUpdate {
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: crane.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp,
        delta_type: DeltaType::Removal,
    });
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: to_node.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp + 1,
        delta_type: DeltaType::Addition,
    });

    let cloned_barrel = Some(barrel.clone());
    state.insert(crane_event.clone(), None);
    state.insert(to_node.clone(), cloned_barrel);

    create_crane_event(crane_event.clone(), to_node.clone(), CraneEventType::Drop, current_timestamp, topic_name, config)
}

pub(crate) fn create_platform_message(
    barrel:Node,
    platform:Node,
    state: &mut BTreeMap<Node, Option<Node>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_names: Vec<String>,
    use_central: bool,
) -> Vec<TopicNameAndUpdate> {
    state.insert(platform.clone(), Some(barrel.clone()));
    //We do this in order to be able to validate the results
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: platform.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp,
        delta_type: DeltaType::Addition,
    });
    create_new_barrel(
        platform.clone(),
        barrel,
        current_timestamp,
        topic_names,
        use_central,
    )
}

pub(crate) fn create_ramp_message(
    barrel: Node,
    ramp: Node,
    state: &mut BTreeMap<Node, Option<Node>>,
    current_timestamp: u64,
    inferred_deltas: &mut Vec<Delta>,
    topic_names: Vec<String>,
    use_central: bool) -> Vec<TopicNameAndUpdate> {
    inferred_deltas.push(Delta {
        src: barrel.clone(),
        trg: ramp.clone(),
        edge_type: "At".to_string(),
        timestamp: current_timestamp,
        delta_type: DeltaType::Removal,
    });
    state.insert(ramp.clone(), None);
    create_remove_barrel_updates(
            ramp,
            barrel.clone(),
            current_timestamp,
            topic_names,
            use_central,
        )
}

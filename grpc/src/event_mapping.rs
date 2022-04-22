
pub(crate) fn from_proto_event(proto_event :crate::event::Event) -> mbei_core::event::Event {
   mbei_core::event::Event {
       event_id: proto_event.event_id,
       timestamp: proto_event.timestamp,
       node_id: proto_event.instance_node_id,
       payload: proto_event.payload
   }
}

pub(crate) fn to_proto_event(event: &mbei_core::event::Event) -> crate::event::Event{
    crate::event::Event {
        event_id: event.event_id.clone(),
        instance_node_id: event.node_id.clone(),
        timestamp: event.timestamp,
        payload: event.payload.clone(),
    }
}

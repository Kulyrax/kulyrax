use kulyrax_core::event::Event;

#[test]
fn event_can_be_created() {
    let e = Event {
        event_id: "evt-1".to_string(),
        event_type: "Test".to_string(),
        payload: "{}".to_string(),
        metadata: Default::default(),
        version: 1,
        sequence: 1,
    };

    assert_eq!(e.event_type, "Test");
}

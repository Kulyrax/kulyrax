use kulyrax_core::event::Event;

#[test]
fn event_can_be_created() {
    let e = Event::new("evt-1".to_string(), "Test".to_string(), 1, "{}".to_string());

    assert_eq!(e.event_type, "Test");
}

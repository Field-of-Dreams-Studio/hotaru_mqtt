//! Protocol tests: keep-alive deadline and ping-interval arithmetic.

use std::sync::Arc;
use std::time::Duration;

use hotaru_core::executable::ExecutableBinding;
use hotaru_core::extensions::ParamsClone;
use hotaru_core::protocol::Protocol;
use hotaru_core::url::{PathPattern, UrlRoot, tokens_to_patterns};

use crate::{MQTT, MqttContext};

use super::matching_endpoint_nodes;
use super::client::client_ping_interval;
use super::server::server_read_deadline;

/// Spec §3.1.2.10: `keep_alive = 0` turns the mechanism off, so the server
/// must not disconnect for inactivity. The old `keep_alive.max(1)` turned
/// that request into the most aggressive deadline the code could produce.
#[test]
fn zero_means_no_deadline_on_either_side() {
    assert_eq!(None, server_read_deadline(0));
    assert_eq!(None, client_ping_interval(0));
}

/// The grace is exactly 1.5×, including where integer division used to lose
/// it: `(1 * 3) / 2` was 1 second, i.e. no grace at all.
#[test]
fn the_grace_is_exact_for_odd_values() {
    assert_eq!(Some(Duration::from_millis(1_500)), server_read_deadline(1));
    assert_eq!(Some(Duration::from_millis(4_500)), server_read_deadline(3));
    assert_eq!(Some(Duration::from_millis(7_500)), server_read_deadline(5));
}

#[test]
fn even_values_are_unchanged() {
    assert_eq!(Some(Duration::from_secs(3)), server_read_deadline(2));
    assert_eq!(Some(Duration::from_secs(90)), server_read_deadline(60));
}

/// The largest legal value must stay well inside `u64` milliseconds.
#[test]
fn the_wire_maximum_does_not_overflow() {
    let d = server_read_deadline(u16::MAX).expect("65535 is not zero");
    assert_eq!(Duration::from_millis(65_535 * 1_500), d);
    assert_eq!(98_302_500, d.as_millis());
}

/// A client pings on its own declared interval, not on the server's grace —
/// pinging at 1.5× would be late by construction.
#[test]
fn the_client_pings_on_its_own_interval_not_the_grace() {
    assert_eq!(Some(Duration::from_secs(60)), client_ping_interval(60));
    assert_eq!(Some(Duration::from_secs(1)), client_ping_interval(1));
    assert!(client_ping_interval(60).unwrap() < server_read_deadline(60).unwrap());
}

type TestRoot = UrlRoot<MqttContext, hotaru_io_tokio::TcpTransport>;

#[allow(deprecated)]
fn register_test_endpoint(root: &TestRoot, path: &str) {
    root.sub_url(
        path,
        ExecutableBinding::new().with_handler(Arc::new(|ctx: MqttContext| async move {
            Ok(ctx)
        })),
        ParamsClone::default(),
    )
    .expect("test endpoint registration");
}

#[test]
fn mqtt_protocol_overrides_the_framework_url_grammar() {
    let tokens = <MQTT as Protocol>::tokenize_url(r"sport/+/a\<b/#").unwrap();
    let (patterns, _) = tokens_to_patterns(&tokens).unwrap();
    assert_eq!(
        patterns,
        vec![
            PathPattern::Literal("sport".into()),
            PathPattern::Any,
            PathPattern::Literal(r"a\<b".into()),
            PathPattern::AnyPath,
        ]
    );
    assert_eq!(
        <MQTT as Protocol>::lit_parser("sport//score"),
        vec!["sport", "", "score"]
    );
}

#[test]
fn endpoint_hash_matches_its_parent_without_running_the_empty_parent() {
    let root = TestRoot::new();
    register_test_endpoint(&root, "sport/<**path>");

    let nodes = matching_endpoint_nodes(&root, "sport");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].path(), &PathPattern::AnyPath);
}

#[test]
fn exact_endpoint_precedes_hash_when_both_match_the_parent() {
    let root = TestRoot::new();
    register_test_endpoint(&root, "sport/<**path>");
    register_test_endpoint(&root, "sport");

    let nodes = matching_endpoint_nodes(&root, "sport");
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].path(), &PathPattern::Literal("sport".into()));
    assert_eq!(nodes[1].path(), &PathPattern::AnyPath);
}

#[test]
fn endpoint_root_wildcards_do_not_match_system_topics() {
    let root = TestRoot::new();
    register_test_endpoint(&root, "<**path>");
    register_test_endpoint(&root, "<mqtt_level>/status");
    register_test_endpoint(&root, "$SYS/<**path>");

    let nodes = matching_endpoint_nodes(&root, "$SYS/status");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].path(), &PathPattern::AnyPath);
}

#[test]
fn endpoint_wildcards_below_the_first_level_may_match_dollar_segments() {
    let root = TestRoot::new();
    register_test_endpoint(&root, "sport/<mqtt_level>");

    assert_eq!(matching_endpoint_nodes(&root, "sport/$value").len(), 1);
}

//! Broker tests: topic-filter matching.

use super::*;

#[test]
fn filter_matches_literal() {
    let segs: Vec<&str> = "a/b/c".split('/').collect();
    assert!(filter_matches("a/b/c", &segs));
    assert!(!filter_matches("a/b/d", &segs));
}

#[test]
fn filter_matches_plus() {
    let segs: Vec<&str> = "a/b/c".split('/').collect();
    assert!(filter_matches("a/+/c", &segs));
    assert!(filter_matches("+/+/+", &segs));
    assert!(!filter_matches("a/+/d", &segs));
    assert!(!filter_matches("a/+", &segs));
}

#[test]
fn filter_matches_hash() {
    let segs: Vec<&str> = "a/b/c".split('/').collect();
    assert!(filter_matches("a/#", &segs));
    assert!(filter_matches("#", &segs));
    assert!(filter_matches("a/b/#", &segs));
    assert!(!filter_matches("b/#", &segs));
}

#[test]
fn filter_matches_partial() {
    let segs: Vec<&str> = "a/b".split('/').collect();
    assert!(!filter_matches("a/b/c", &segs));
    assert!(filter_matches("a/b", &segs));
}

#[test]
fn leading_wildcards_do_not_match_system_topics() {
    let segs: Vec<&str> = "$SYS/monitor/Clients".split('/').collect();
    assert!(!filter_matches("#", &segs));
    assert!(!filter_matches("+/monitor/Clients", &segs));
}

#[test]
fn literal_system_prefix_and_nonleading_wildcards_still_match() {
    let system: Vec<&str> = "$SYS/monitor/Clients".split('/').collect();
    assert!(filter_matches("$SYS/#", &system));

    let embedded_dollar: Vec<&str> = "sport/$value".split('/').collect();
    assert!(filter_matches("sport/+", &embedded_dollar));
    assert!(filter_matches("sport/#", &embedded_dollar));
}

#[test]
fn hash_matches_its_parent_level() {
    let segs: Vec<&str> = "sport".split('/').collect();
    assert!(filter_matches("sport/#", &segs));
}

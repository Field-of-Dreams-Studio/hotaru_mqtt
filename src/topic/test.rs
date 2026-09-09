//! Topic tests: filter/path parsing and wildcard rules.

use super::*;

#[test]
fn subscribe_literal() {
    let p = parse_subscribe_filter("sensors/temp").unwrap();
    assert_eq!(
        p,
        vec![
            PathPattern::Literal("sensors".into()),
            PathPattern::Literal("temp".into())
        ]
    );
}

#[test]
fn subscribe_plus() {
    let p = parse_subscribe_filter("sensors/+/temp").unwrap();
    assert_eq!(
        p,
        vec![
            PathPattern::Literal("sensors".into()),
            PathPattern::Any,
            PathPattern::Literal("temp".into())
        ]
    );
}

#[test]
fn subscribe_hash() {
    let p = parse_subscribe_filter("sensors/#").unwrap();
    assert_eq!(
        p,
        vec![PathPattern::Literal("sensors".into()), PathPattern::AnyPath]
    );
}

#[test]
fn subscribe_hash_only() {
    let p = parse_subscribe_filter("#").unwrap();
    assert_eq!(p, vec![PathPattern::AnyPath]);
}

#[test]
fn subscribe_rejects_hash_not_terminal() {
    let e = parse_subscribe_filter("a/#/b").unwrap_err();
    assert!(matches!(
        e,
        MqttError::Protocol(Violation::HashWildcardNotTerminal)
    ));
}

#[test]
fn subscribe_rejects_mixed_wildcard() {
    let e = parse_subscribe_filter("a+b").unwrap_err();
    assert!(matches!(
        e,
        MqttError::Protocol(Violation::WildcardMixedWithLiteral)
    ));
}

#[test]
fn publish_rejects_wildcards() {
    let e = parse_publish_topic("a/+/b").unwrap_err();
    assert!(matches!(
        e,
        MqttError::Protocol(Violation::WildcardInPublishTopic)
    ));
}

#[test]
fn publish_accepts_dollar_sys() {
    let p = parse_publish_topic("$SYS/broker/clients").unwrap();
    assert_eq!(p.len(), 3);
}

#[test]
fn path_to_wire_round_trip() {
    let path = parse_subscribe_filter("sensors/+/temp").unwrap();
    assert_eq!(path_to_wire_filter(&path), "sensors/+/temp");

    let path = parse_subscribe_filter("a/#").unwrap();
    assert_eq!(path_to_wire_filter(&path), "a/#");
}

#[test]
fn validation_uses_the_topic_grammars() {
    assert!(validate_subscribe_filter("a/+/b").is_ok());
    assert!(validate_subscribe_filter("a/#/b").is_err());
    assert!(validate_publish_topic("a/b/c").is_ok());
    assert!(validate_publish_topic("a/+").is_err());
}

#[test]
fn framework_tokens_use_the_mqtt_filter_grammar() {
    let tokens = tokenize_subscribe_filter(r"sensors/+/a\<b/#").unwrap();
    let (patterns, _) = hotaru_core::url::tokens_to_patterns(&tokens).unwrap();
    assert_eq!(
        patterns,
        vec![
            PathPattern::Literal("sensors".into()),
            PathPattern::Any,
            PathPattern::Literal(r"a\<b".into()),
            PathPattern::AnyPath,
        ]
    );
}

#[test]
fn framework_tokens_reject_invalid_mqtt_filters() {
    assert!(tokenize_subscribe_filter("a/#/b").is_err());
    assert!(tokenize_subscribe_filter("a+b").is_err());
}

#[test]
fn literal_split_preserves_empty_levels() {
    assert_eq!(split_topic_literal("a//b/"), vec!["a", "", "b", ""]);
    assert!(split_topic_literal("").is_empty());
}

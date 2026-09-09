//! MQTT topic and filter parsing / validation.
//!
//! Two grammars:
//!
//! - **SUBSCRIBE filter** (per MQTT 3.1.1 §4.7): accepts `+` (single-level
//!   wildcard) and `#` (multi-level wildcard, must be terminal).
//! - **PUBLISH topic** (per §3.3): rejects `+` / `#`; all segments are literal.
//!
//! Both parsers produce `Vec<PathPattern>` compatible with `hotaru_core`'s
//! URL router — `+` maps to `PathPattern::Any`, `#` to `PathPattern::AnyPath`.
//!
//! The reverse map `path_to_wire_filter` converts a registered URL path
//! (HTTP grammar — `<id>` / `<**path>`) back to MQTT wire format (`+` / `#`),
//! used when constructing SUBSCRIBE packets from `endpoint!`-registered paths.

use hotaru_core::url::{PathPattern, PatternError, RawToken, TypeKind};

use crate::error::{MqttError, Violation};

const MAX_TOPIC_LEN: usize = 65535;

/// Parse a SUBSCRIBE topic filter into `Vec<PathPattern>`.
///
/// Accepts `+` (single-level wildcard) and `#` (multi-level wildcard, must be
/// the terminal segment). Mixed forms like `a+b` or `a/#/b` are rejected.
pub fn parse_subscribe_filter(filter: &str) -> Result<Vec<PathPattern>, MqttError> {
    if filter.is_empty() {
        return Err(Violation::EmptyTopic.into());
    }
    if filter.len() > MAX_TOPIC_LEN {
        return Err(Violation::TopicTooLong.into());
    }

    let segments: Vec<&str> = filter.split('/').collect();
    let last_idx = segments.len() - 1;
    let mut patterns = Vec::with_capacity(segments.len());

    for (i, seg) in segments.iter().enumerate() {
        match *seg {
            "+" => patterns.push(PathPattern::Any),
            "#" => {
                if i != last_idx {
                    return Err(Violation::HashWildcardNotTerminal.into());
                }
                patterns.push(PathPattern::AnyPath);
            }
            s if s.contains('+') || s.contains('#') => {
                return Err(Violation::WildcardMixedWithLiteral.into());
            }
            s => patterns.push(PathPattern::Literal(s.to_string())),
        }
    }
    Ok(patterns)
}

/// Tokenize an MQTT topic filter for the framework registration pipeline.
///
/// `Protocol::tokenize_url` has to return the framework's HTTP-shaped token
/// stream, so validate with the MQTT grammar first and then encode only the
/// three MQTT segment kinds into that stream. In particular, this never runs
/// the HTTP escape rules: a backslash in an MQTT topic remains a backslash.
pub(crate) fn tokenize_subscribe_filter(
    filter: &str,
) -> Result<Vec<RawToken>, PatternError> {
    let patterns = parse_subscribe_filter(filter).map_err(|_| {
        PatternError::UnexpectedToken {
            at: 0,
            token: RawToken::Literal(filter.to_string()),
        }
    })?;

    let mut tokens = Vec::with_capacity(patterns.len().saturating_mul(2));
    for (index, pattern) in patterns.into_iter().enumerate() {
        if index != 0 {
            tokens.push(RawToken::Slash);
        }
        match pattern {
            PathPattern::Literal(segment) => tokens.push(RawToken::Literal(segment)),
            PathPattern::Any => tokens.extend([
                RawToken::AngleStart,
                RawToken::Ident("mqtt_level".to_string()),
                RawToken::AngleClose,
            ]),
            PathPattern::AnyPath => tokens.extend([
                RawToken::AngleStart,
                RawToken::Type(TypeKind::Path),
                RawToken::AngleClose,
            ]),
            PathPattern::Regex(_) => {
                unreachable!("the MQTT topic-filter grammar never emits regex segments")
            }
        }
    }
    Ok(tokens)
}

/// Split an incoming MQTT topic into the segments consumed by the URL walker.
pub(crate) fn split_topic_literal(topic: &str) -> Vec<&str> {
    if topic.is_empty() {
        Vec::new()
    } else {
        topic.split('/').collect()
    }
}

/// Parse a PUBLISH topic into `Vec<PathPattern>` (all literal segments).
///
/// Rejects topics containing `+` or `#`.
pub fn parse_publish_topic(topic: &str) -> Result<Vec<PathPattern>, MqttError> {
    if topic.is_empty() {
        return Err(Violation::EmptyTopic.into());
    }
    if topic.len() > MAX_TOPIC_LEN {
        return Err(Violation::TopicTooLong.into());
    }

    let mut patterns = Vec::new();
    for seg in topic.split('/') {
        if seg.contains('+') || seg.contains('#') {
            return Err(Violation::WildcardInPublishTopic.into());
        }
        patterns.push(PathPattern::Literal(seg.to_string()));
    }
    Ok(patterns)
}

/// Convert a registered `Vec<PathPattern>` back to MQTT wire filter format.
///
/// Mapping:
/// - `Literal(s)` → `s`
/// - `Any` → `"+"`
/// - `Regex(_)` → `"+"` (regex has no MQTT wire equivalent; degrades to single-level)
/// - `AnyPath` → `"#"`
pub fn path_to_wire_filter(path: &[PathPattern]) -> String {
    path.iter()
        .map(|p| match p {
            PathPattern::Literal(s) => s.clone(),
            PathPattern::Any | PathPattern::Regex(_) => "+".to_string(),
            PathPattern::AnyPath => "#".to_string(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Validate a SUBSCRIBE filter with the same grammar used by registration and
/// broker matching.
pub fn validate_subscribe_filter(filter: &str) -> Result<(), MqttError> {
    parse_subscribe_filter(filter).map(drop)
}

/// Validate a PUBLISH topic with the same grammar used by its parser.
pub fn validate_publish_topic(topic: &str) -> Result<(), MqttError> {
    parse_publish_topic(topic).map(drop)
}

#[cfg(test)]
mod test;

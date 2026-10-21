//! The read-only information console: build-time-stamped facts about the
//! build, the Maven `version.properties` shape (`key=value` lines,
//! LF-terminated). Read-only by construction — no node, no handle, no
//! state — and an unavailable fact is reported `unknown`, never guessed.

/// The release tag naming this build, or `unknown`.
pub const VERSION: &str = env!("LUNET_INFO_VERSION");

/// The commit this build was made from, twelve hex digits, or `unknown`.
pub const SHA: &str = env!("LUNET_INFO_SHA");

/// The build's feature shape: `production`, or `production` plus the
/// development features this build carries (`+flight-recorder`). A
/// production build is exactly `production`.
pub const FEATURE_SHAPE: &str = env!("LUNET_INFO_FEATURES");

/// Whether this build's working tree carried uncommitted changes; an
/// unproven clean tree is never reported clean.
#[must_use]
pub fn dirty() -> bool {
    env!("LUNET_INFO_DIRTY") == "true"
}

/// The property names, in the order [`properties`] carries them.
pub const KEYS: [&str; 4] = ["version", "sha", "dirty", "feature_shape"];

/// One property by name, or `None` for a name the console does not carry.
#[must_use]
pub fn field(name: &str) -> Option<&'static str> {
    match name {
        "version" => Some(VERSION),
        "sha" => Some(SHA),
        "dirty" => Some(if dirty() { "true" } else { "false" }),
        "feature_shape" => Some(FEATURE_SHAPE),
        _ => None,
    }
}

/// The whole payload: the Maven `version.properties` text, LF-terminated.
#[must_use]
pub fn properties() -> String {
    let mut text = String::with_capacity(96);
    for key in KEYS {
        let value = field(key).expect("every carried key has a field");
        text.push_str(key);
        text.push('=');
        text.push_str(value);
        text.push('\n');
    }
    text
}

/// The `GET /info` response: the properties text as a complete HTTP/1.1
/// message.
#[must_use]
pub fn info_response() -> String {
    let body = properties();
    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_carried_key_has_a_field() {
        for key in KEYS {
            assert!(field(key).is_some(), "{key} has a field");
        }
        assert_eq!(field("lock_state"), None, "the console names no other");
        assert_eq!(field("request"), None, "the console has no write verb");
        assert_eq!(field("node"), None, "the console holds no node");
    }

    #[test]
    fn the_properties_are_one_key_per_line() {
        let text = properties();
        assert!(text.ends_with('\n'), "the payload is LF-terminated");
        let lines: Vec<&str> = text.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), KEYS.len());
        for (line, key) in lines.iter().zip(KEYS) {
            let (named, value) = line.split_once('=').expect("a key=value line");
            assert_eq!(*named, *key);
            assert!(!value.is_empty(), "{key} carries a value");
        }
    }

    #[test]
    fn the_properties_agree_with_the_fields() {
        let text = properties();
        for key in KEYS {
            let expected = format!("{key}={}\n", field(key).expect("a carried key"));
            assert!(text.contains(&expected), "the payload carries {expected:?}");
        }
    }

    #[test]
    fn the_stamped_facts_have_their_shape() {
        assert!(
            VERSION == "unknown" || VERSION.starts_with('v'),
            "a stamped version is a v-tag: {VERSION}"
        );
        assert!(
            SHA == "unknown" || (SHA.len() == 12 && SHA.chars().all(|c| c.is_ascii_hexdigit())),
            "a stamped sha is twelve hex digits: {SHA}"
        );
        assert!(
            FEATURE_SHAPE == "production" || FEATURE_SHAPE.starts_with("production+"),
            "the feature shape names production first: {FEATURE_SHAPE}"
        );
    }

    #[test]
    fn the_response_frames_the_properties() {
        let response = info_response();
        let (head, body) = response.split_once("\r\n\r\n").expect("a header break");
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "the status line");
        assert!(head.contains("Content-Type: text/plain; charset=utf-8\r\n"));
        assert!(
            head.ends_with("Connection: close"),
            "the response closes the connection: {head}"
        );
        let declared: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .expect("the declared length")
            .parse()
            .expect("a decimal length");
        assert_eq!(declared, body.len(), "the declared length is the body's");
        assert_eq!(body, properties(), "the body IS the properties payload");
    }
}

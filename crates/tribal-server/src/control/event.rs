//! Projecting a [`ControlEvent`] onto the JSON-RPC notification frame the socket
//! writes to a subscriber.
//!
//! A [`ControlEvent`] serialises to `{ method, params }` — exactly the body of a
//! server-initiated [`ControlNotification`] once the frozen `jsonrpc` marker is
//! added. Keeping the projection in one place means every publisher frames an
//! event the same way.

use tribal_wire::control::{ControlEvent, ControlNotification, JsonRpcVersion};

/// Projects a control event onto the notification frame the socket sends to a
/// subscriber.
pub(crate) fn notification_for(event: &ControlEvent) -> ControlNotification {
    let projected = serde_json::to_value(event).expect("a control event serialises to JSON");
    let method = projected
        .get("method")
        .and_then(serde_json::Value::as_str)
        .expect("a control event projects a method")
        .to_owned();
    let params = projected.get("params").cloned();
    ControlNotification {
        jsonrpc: JsonRpcVersion,
        method,
        params,
    }
}

#[cfg(test)]
mod tests {
    use tribal_wire::control::WriteEffect;

    use super::*;

    #[test]
    fn test_a_fielded_event_projects_method_and_params() {
        let notification = notification_for(&ControlEvent::ConfigChanged {
            keys: vec!["logging.level".to_owned()],
            effect: WriteEffect::Live,
        });
        assert_eq!(notification.method, "config.changed");
        assert_eq!(
            notification.params.expect("params present")["effect"],
            serde_json::json!("live"),
        );
    }

    #[test]
    fn test_a_fieldless_event_projects_no_params() {
        let notification = notification_for(&ControlEvent::ServerStatusChanged);
        assert_eq!(notification.method, "server.statusChanged");
        assert!(
            notification.params.is_none(),
            "a fieldless event carries no params",
        );
    }
}

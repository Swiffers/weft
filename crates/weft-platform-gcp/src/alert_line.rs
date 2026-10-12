//! The alert line: what weft logs under
//! [`weft_platform_traits::ALERT_TARGET`] when its own work is stuck,
//! written to stderr as one JSON object, which Cloud Run hands to Cloud
//! Logging as a structured entry at severity ERROR carrying the marker
//! field the install's alert policy matches (`deploy/terraform/gcp/alerts.tf`).

use std::io::Write as _;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::{Context, Layer};

/// The field every alert line carries, set to `true`.
// SYNC: the marker field <-> deploy/terraform/gcp/alerts.tf (the policy's log filter)
pub const MARKER: &str = "weft_alert";

/// The layer that writes the alert lines to stderr, and sees nothing
/// else.
pub fn layer<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>() -> impl Layer<S> + Send + Sync {
    AlertLines { writer: std::io::stderr }.with_filter(only_alerts())
}

fn only_alerts() -> Targets {
    Targets::new().with_target(weft_platform_traits::ALERT_TARGET, Level::ERROR)
}

/// Writes each event it is given as one alert line to `writer`.
struct AlertLines<W> {
    writer: W,
}

impl<S: Subscriber, W: for<'w> MakeWriter<'w> + 'static> Layer<S> for AlertLines<W> {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let mut line = fields.0;
        line.insert("severity".into(), "ERROR".into());
        line.insert(MARKER.into(), true.into());
        // A line stderr refuses has nowhere left to be reported: stderr is
        // where it would go.
        let _ = writeln!(self.writer.make_writer(), "{}", serde_json::Value::Object(line));
    }
}

/// An event's fields as JSON, its message under `message`, the key
/// Cloud Logging shows an entry by.
#[derive(Default)]
struct Fields(serde_json::Map<String, serde_json::Value>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}").into());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::SubscriberExt as _;

    use super::*;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for Buffer {
        type Writer = Buffer;
        fn make_writer(&'w self) -> Buffer {
            self.clone()
        }
    }

    #[test]
    fn an_alert_is_one_structured_error_line_with_the_marker_and_nothing_else_is_written() {
        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::registry().with(AlertLines { writer: buffer.clone() }.with_filter(only_alerts()));
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(target: weft_platform_traits::ALERT_TARGET, project = %uuid::Uuid::from_u128(7), since_ms = 12_i64, "weft's own work is stuck: infra keeps failing");
            tracing::error!(target: "weft_runtime", "an ordinary error");
            tracing::warn!(target: weft_platform_traits::ALERT_TARGET, "below the alert's level");
        });
        let written = String::from_utf8(buffer.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = written.lines().collect();
        assert_eq!(lines.len(), 1, "only the alert is written: {written}");
        let line: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(
            line,
            serde_json::json!({
                "severity": "ERROR",
                "weft_alert": true,
                "message": "weft's own work is stuck: infra keeps failing",
                "project": uuid::Uuid::from_u128(7).to_string(),
                "since_ms": 12,
            })
        );
    }
}

# An email when weft's own work is stuck: something `weft status` lists
# under `waiting on weft:` that weft gave up retrying, or that it still
# retries var.alert_after_minutes after it began. weft logs one line per
# stuck failure, at severity ERROR with the field `weft_alert`
# (crates/weft-platform-gcp/src/alert_line.rs), and logs it again only once
# that failure cleared and came back. This policy matches that line and
# emails var.alert_email. var.alerts_enabled = false makes neither, and
# weft logs no such line.
#
# SYNC: alerts <-> serverless.tf (platform.alerts in local.install_config)

resource "google_monitoring_notification_channel" "email" {
  count        = var.alerts_enabled ? 1 : 0
  display_name = "${var.name}: weft's own work is stuck"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
  depends_on = [google_project_service.apis]

  lifecycle {
    precondition {
      condition     = var.alert_email != ""
      error_message = "alerts_enabled is true, and no email to send the alerts to was found: no person holds the Owner role directly on the project. Set the repository secret WEFT_ALERT_EMAIL to the address, or set alerts_enabled to false in variables.tf."
    }
  }
}

resource "google_monitoring_alert_policy" "stuck" {
  count        = var.alerts_enabled ? 1 : 0
  display_name = "${var.name}: weft's own work is stuck"
  combiner     = "OR"

  conditions {
    display_name = "weft logged that its own work is stuck"
    condition_matched_log {
      # SYNC: the marker field <-> crates/weft-platform-gcp/src/alert_line.rs (MARKER)
      filter = "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=~\"^${var.name}-role-\" AND severity=ERROR AND jsonPayload.weft_alert=true"
      label_extractors = {
        project = "EXTRACT(jsonPayload.project)"
      }
    }
  }

  # One email per five minutes at most, however many lines match.
  alert_strategy {
    notification_rate_limit {
      period = "300s"
    }
    auto_close = "1800s"
  }

  notification_channels = [google_monitoring_notification_channel.email[0].id]

  documentation {
    mime_type = "text/markdown"
    content   = "weft's own work is stuck. The log line that raised this says what failed and why; `weft status --on <target>` in the project shows it under `waiting on weft:`, with what happens next."
  }

  depends_on = [google_project_service.apis]
}

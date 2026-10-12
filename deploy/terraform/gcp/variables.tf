# SYNC: the variables the install workflow sets (project_id, region, zone, database_url, database_listen_url, runtime_image, builder_base_image, weft_repository, weft_commit, access_apps_json, alert_email) <-> .github/workflows/install-gcp.yml (the TF_VAR_* names)

variable "project_id" {
  description = "The GCP project the install lives in."
  type        = string
}

variable "region" {
  description = "Where everything lives."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "The zone infra machines run in, inside `region`."
  type        = string
  default     = "us-central1-a"
}

variable "name" {
  description = "Prefix of every resource this creates, so two installs can share a project."
  type        = string
  default     = "weft"
}

variable "database_url" {
  description = "The Postgres the install keeps everything in, as a connection URL (postgres://user:password@host/db?sslmode=require). Any Postgres works; one that scales to zero costs nothing while the install is idle. A pooled address is fine."
  type        = string
  sensitive   = true
}

variable "database_listen_url" {
  description = "A direct (session) address of the same database, when database_url goes through a pooler that hands out a connection per transaction: a LISTEN needs a session of its own (the runtime refuses to start, naming this, when it cannot listen). Empty when database_url is already one."
  type        = string
  default     = ""
  sensitive   = true
}

variable "signals_per_holder" {
  description = "The most held signals (sockets, streams, subscriptions kept open) one holder takes; weft runs one more holder per this many."
  type        = number
  default     = 200
}

variable "holder_cpu" {
  description = "The CPU of each holder."
  type        = string
  default     = "1"
}

variable "holder_memory" {
  description = "The memory of each holder."
  type        = string
  default     = "512Mi"
}

variable "runtime_image" {
  description = "The weft-runtime image every role runs (the install workflow builds it and passes its ref)."
  type        = string
}

variable "builder_base_image" {
  description = "The image a project's worker compiles in (the install workflow builds it and passes its ref)."
  type        = string
}

variable "compile_lanes" {
  description = "How many project builds run side by side (each is one Cloud Build job)."
  type        = number
  default     = 2
}

variable "build_machine" {
  description = "The Cloud Build machine a program's worker image compiles on. Empty is Cloud Build's default machine, which its free build minutes cover; E2_HIGHCPU_8 or E2_HIGHCPU_32 compile faster and are paid by the minute. An image is built once per version of a program, never per worker started."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "E2_MEDIUM", "E2_HIGHCPU_8", "E2_HIGHCPU_32", "N1_HIGHCPU_8", "N1_HIGHCPU_32"], var.build_machine)
    error_message = "build_machine is empty (the free default) or one of E2_MEDIUM, E2_HIGHCPU_8, E2_HIGHCPU_32, N1_HIGHCPU_8, N1_HIGHCPU_32."
  }
}

variable "invalid_tokens_per_minute" {
  description = "Refused tokens one address may present per minute on the token doors before every token door refuses it for the rest of the minute. 0 turns the block off."
  type        = number
  default     = 30
}

variable "weft_repository" {
  description = "The weft repository (owner/name) the install runs, so a project's CI builds the same CLI."
  type        = string
}

variable "weft_commit" {
  description = "The weft commit the install runs."
  type        = string
}

variable "access_apps_json" {
  description = "The access-apps.json holding the OAuth apps and the runtime's own provider keys, or empty for none."
  type        = string
  default     = ""
  sensitive   = true
}

variable "wakes_retry_window" {
  description = "How long the wakes queue keeps retrying a wake its receiver fails (a user's timer, cron, poll or provider renewal, and a holder's liveness check). \"0s\" is no limit short of Cloud Tasks' own task retention: each of these wakes sets the next, so one dropped ends its chain for good. The listener itself stops after var.wake_attempts deliveries of a trigger's wake, giving the trigger up where its user sees it."
  type        = string
  default     = "0s"
}

variable "wake_attempts" {
  description = "How many deliveries of one trigger's wake (a timer, cron, poll or provider renewal) the listener answers while it keeps failing; a delivery that never reached it (the machine down, a 5xx from in front of it) is not counted. A failure on the last gives the trigger up, shown on its node, until a resync of its project (weft resync) registers it again, or, for one that signs in through a connection, that connection is reconnected. The holder's liveness check on the same queue is not bounded by it. With the default backoff, 20 is about an hour of failing tries."
  type        = number
  default     = 20

  validation {
    condition     = var.wake_attempts >= 1
    error_message = "wake_attempts must be 1 or more: every wake is delivered at least once."
  }
}

variable "wakes_longest_backoff" {
  description = "The longest pause between two tries of a wake the wakes queue keeps retrying. It is also how late a cron, a poll or a holder's liveness check can resume once its receiver is back after an outage."
  type        = string
  default     = "300s"
}

variable "alerts_enabled" {
  description = "Whether weft emails you when its own work is stuck (what `weft status` lists under `waiting on weft:`): one Cloud Monitoring alert policy on the line weft logs then, and an email channel. false makes neither, and weft logs no such line."
  type        = bool
  default     = true
}

variable "alert_after_minutes" {
  description = "How many minutes weft's own work may keep failing while weft still retries it before it counts as stuck and alerts. Work weft gave up retrying alerts at once."
  type        = number
  default     = 5

  validation {
    condition     = var.alert_after_minutes >= 0 && floor(var.alert_after_minutes) == var.alert_after_minutes
    error_message = "alert_after_minutes is a whole number of minutes, 0 or more."
  }
}

variable "alert_email" {
  description = "Where the alerts are emailed. The install workflow sets it from the repository secret WEFT_ALERT_EMAIL, or else to the first person (by address) holding the Owner role directly on the project, usually the Google account that made it. Needed when alerts_enabled is true."
  type        = string
  default     = ""
}

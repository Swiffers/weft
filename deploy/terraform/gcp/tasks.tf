# The two queues every wake goes through. Each wake is one task, delivered
# once to the role that set it, with the core account's identity.
#
# What keeps a user's triggers going (a timer, a cron, a poll, a provider
# renewal, all sent to the listener's /wake, and a holder's liveness
# check, sent to the broker) goes through the wakes queue, which keeps
# retrying without a time limit by default (var.wakes_retry_window): each
# of these wakes sets the next, so one dropped would end its chain for
# good, while a stale one is a no-op. The listener reads each delivery's
# execution count (the earlier deliveries it answered, 5xx answers left
# out) and, on the last of var.wake_attempts, gives a failing
# trigger up where its user sees it and answers the wake as handled, as it
# does at once for one that can never be handled. weft's own looks (a
# serverless role's tick for one of its loops, a ring that failed) go
# through the ticks queue, which gives up after ten minutes.

resource "google_cloud_tasks_queue" "wakes" {
  name       = "${var.name}-wakes"
  location   = var.region
  depends_on = [google_project_service.apis]

  retry_config {
    max_attempts       = -1
    max_retry_duration = var.wakes_retry_window
    min_backoff        = "1s"
    max_backoff        = var.wakes_longest_backoff
    max_doublings      = 8
  }
}

resource "google_cloud_tasks_queue" "ticks" {
  name       = "${var.name}-ticks"
  location   = var.region
  depends_on = [google_project_service.apis]

  retry_config {
    max_attempts       = -1
    max_retry_duration = "600s"
    min_backoff        = "1s"
    max_backoff        = "60s"
    max_doublings      = 8
  }
}

# weft deletes a wake it no longer wants (a loop's look moved, a holder's
# liveness check replaced), a right the enqueuer role the core holds
# across the project does not carry.
resource "google_cloud_tasks_queue_iam_member" "core_deletes_wakes" {
  for_each = {
    wakes = google_cloud_tasks_queue.wakes
    ticks = google_cloud_tasks_queue.ticks
  }
  name     = each.value.name
  location = each.value.location
  role     = "roles/cloudtasks.taskDeleter"
  member   = local.core
}

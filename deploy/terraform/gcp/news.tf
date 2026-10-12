# What happens to a run a worker drives (an answer handed to it, a cancel),
# told across weft's copies without the database, so the database can sleep
# while a run only waits (crates/weft-platform-gcp/src/news.rs). The copies
# that write (the dispatcher's and the broker's) announce on the topic what
# their writes left in the outbox; every broker copy makes a subscription of
# its own to it while it runs, pulls from it, and deletes it as it stops.
# Both roles run as the core account.

resource "google_pubsub_topic" "news" {
  name       = "${var.name}-news"
  depends_on = [google_project_service.apis]
}

# The copies that write announce.
resource "google_pubsub_topic_iam_member" "core_announces_news" {
  topic  = google_pubsub_topic.news.id
  role   = "roles/pubsub.publisher"
  member = local.core
}

# A broker copy attaches its subscription to this topic (and a holder copy
# to the holders topic below), and to no other.
resource "google_project_iam_custom_role" "news_attach" {
  role_id     = "${replace(var.name, "-", "_")}_news_attach"
  title       = "weft news: attach a subscription"
  permissions = ["pubsub.topics.attachSubscription"]
}

resource "google_pubsub_topic_iam_member" "core_attaches_to_news" {
  topic  = google_pubsub_topic.news.id
  role   = google_project_iam_custom_role.news_attach.id
  member = local.core
}

# What the holders need to hear without the database: the set of held
# signals changed, or a connection's access changed. The dispatcher
# announces it here, and every holder copy makes a subscription of its own
# to this topic while it runs and pulls from it, as the broker copies do
# with the news topic. The holders run as the core account too.
resource "google_pubsub_topic" "holders" {
  name       = "${var.name}-holders"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic_iam_member" "core_announces_to_holders" {
  topic  = google_pubsub_topic.holders.id
  role   = "roles/pubsub.publisher"
  member = local.core
}

resource "google_pubsub_topic_iam_member" "core_attaches_to_holders" {
  topic  = google_pubsub_topic.holders.id
  role   = google_project_iam_custom_role.news_attach.id
  member = local.core
}

# Making, pulling from and deleting a copy's own subscription, the broker
# copies' on the news topic and the holder copies' on the holders one. Its
# name is made while it runs, so these rights cannot sit on the
# subscription itself. An IAM Condition cannot match Pub/Sub subscriptions by name
# either: of Pub/Sub, only Pub/Sub Lite is among the services whose
# resources IAM Conditions' resource attributes cover. So these three are
# granted on the project, and the core account may also consume and delete
# the install's other subscriptions there (the supervisor's push one).
resource "google_project_iam_custom_role" "news_subscriptions" {
  role_id = "${replace(var.name, "-", "_")}_news_subscriptions"
  title   = "weft news subscriptions"
  permissions = [
    "pubsub.subscriptions.create",
    "pubsub.subscriptions.consume",
    "pubsub.subscriptions.delete",
  ]
}

resource "google_project_iam_member" "core_news_subscriptions" {
  project = var.project_id
  role    = google_project_iam_custom_role.news_subscriptions.id
  member  = local.core
}

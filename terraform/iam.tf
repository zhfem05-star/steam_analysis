resource "google_service_account" "gke_nodes" {
  account_id   = "steam-analysis-${var.environment}-node"
  display_name = "GKE node service account (steam-analysis ${var.environment})"
}

# 노드가 필요로 하는 최소 권한만 부여 (기본 Compute Engine SA의 광범위한 편집 권한 대신)
resource "google_project_iam_member" "gke_nodes_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "gke_nodes_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_project_iam_member" "gke_nodes_artifact_registry" {
  project = var.project_id
  role    = "roles/artifactregistry.reader"
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

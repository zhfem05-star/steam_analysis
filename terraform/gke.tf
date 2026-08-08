resource "google_container_cluster" "main" {
  name     = "steam-analysis-${var.environment}"
  location = var.zone # 존 클러스터 — 리전 클러스터보다 관리 비용 저렴

  # 기본 노드풀은 바로 제거하고 아래 커스텀 노드풀만 사용
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.private.id

  ip_allocation_policy {
    cluster_secondary_range_name  = "gke-pods"
    services_secondary_range_name = "gke-services"
  }

  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = false # 로컬 kubectl 접근을 위해 control plane 공인 endpoint는 유지
    master_ipv4_cidr_block  = "172.16.0.0/28"
  }

  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  release_channel {
    channel = "REGULAR"
  }

  deletion_protection = false
}

# 일반 워크로드용 (Airflow 등)
resource "google_container_node_pool" "default" {
  name     = "default-pool"
  cluster  = google_container_cluster.main.name
  location = var.zone

  node_count = 1

  node_config {
    machine_type    = "e2-standard-4"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# Kafka broker 배치용 노드풀.
# 아직 실제 워크로드가 없어서 min 0으로 잡아 컴퓨트 비용 방지.
# 나중에 StatefulSet + PVC로 브로커를 올릴 때 이 taint를 toleration으로 타겟팅.
resource "google_container_node_pool" "kafka" {
  name     = "kafka-pool"
  cluster  = google_container_cluster.main.name
  location = var.zone

  autoscaling {
    min_node_count = 0
    max_node_count = 3
  }

  node_config {
    machine_type    = "e2-standard-4"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    labels = {
      workload = "kafka"
    }

    taint {
      key    = "workload"
      value  = "kafka"
      effect = "NO_SCHEDULE"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

# Spark executor 배치용 노드풀. 배치성 워크로드라 오토스케일링 폭을 크게 잡음.
resource "google_container_node_pool" "spark" {
  name     = "spark-pool"
  cluster  = google_container_cluster.main.name
  location = var.zone

  autoscaling {
    min_node_count = 0
    max_node_count = 5
  }

  node_config {
    machine_type    = "e2-standard-8"
    service_account = google_service_account.gke_nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    labels = {
      workload = "spark"
    }

    taint {
      key    = "workload"
      value  = "spark"
      effect = "NO_SCHEDULE"
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }
}

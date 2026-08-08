# VPC 내부 통신 전부 허용 (Kafka broker간, Spark driver-executor간, Airflow-DB간 통신 등)
resource "google_compute_firewall" "allow_internal" {
  name    = "steam-analysis-${var.environment}-allow-internal"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
  }
  allow {
    protocol = "udp"
  }
  allow {
    protocol = "icmp"
  }

  source_ranges = [
    google_compute_subnetwork.private.ip_cidr_range,
    "10.20.0.0/14", # gke-pods
    "10.30.0.0/20", # gke-services
  ]
}

# IAP(Identity-Aware Proxy) 경유 SSH만 허용 — 공인 IP로 직접 SSH 접근 금지
resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "steam-analysis-${var.environment}-allow-iap-ssh"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  # GCP IAP의 고정 소스 대역 (변경되지 않는 GCP 예약 대역)
  source_ranges = ["35.235.240.0/20"]
}

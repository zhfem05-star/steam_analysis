resource "google_compute_network" "vpc" {
  name                    = "steam-analysis-${var.environment}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "private" {
  name          = "steam-analysis-${var.environment}-private-subnet"
  ip_cidr_range = "10.10.0.0/20"
  region        = var.region
  network       = google_compute_network.vpc.id

  # GKE Pod/Service용 secondary range.
  # Kafka/Spark가 나중에 Pod로 늘어날 걸 감안해 넉넉하게 잡아둠.
  secondary_ip_range {
    range_name    = "gke-pods"
    ip_cidr_range = "10.20.0.0/14"
  }
  secondary_ip_range {
    range_name    = "gke-services"
    ip_cidr_range = "10.30.0.0/20"
  }

  private_ip_google_access = true
}

# 프라이빗 GKE 노드가 외부(Steam API 호출, 컨테이너 이미지 pull 등)로 나갈 수 있게 해주는 NAT.
# 인바운드는 열지 않음 — 아웃바운드 전용.
resource "google_compute_router" "router" {
  name    = "steam-analysis-${var.environment}-router"
  region  = var.region
  network = google_compute_network.vpc.id
}

resource "google_compute_router_nat" "nat" {
  name                               = "steam-analysis-${var.environment}-nat"
  router                             = google_compute_router.router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
}

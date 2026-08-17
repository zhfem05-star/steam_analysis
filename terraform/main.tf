terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # 리소스가 늘어나 팀/CI에서 state를 공유해야 할 때 GCS backend로 전환
  # backend "gcs" {
  #   bucket = "steam-analysis-tfstate"
  #   prefix = "terraform/state"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

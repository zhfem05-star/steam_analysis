variable "project_id" {
  description = "GCP 프로젝트 ID"
  type        = string
}

variable "region" {
  description = "리소스를 배치할 리전"
  type        = string
  default     = "asia-northeast3"
}

variable "zone" {
  description = "존 단위 리소스(GKE 클러스터/노드풀)에 사용할 존"
  type        = string
  default     = "asia-northeast3-a"
}

variable "environment" {
  description = "리소스 이름에 붙일 환경 식별자"
  type        = string
  default     = "dev"
}

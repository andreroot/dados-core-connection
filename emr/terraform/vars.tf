variable "job_flow_role_name" {
  type    = string
  default = "EphemeralEMRJobFlowRole"
}

variable "service_role_name" {
  type    = string
  default = "EphemeralEMRServiceRole"
}

variable "security_group_name" {
  type    = string
  default = "EphemeralEMRSecurityGroup"
}

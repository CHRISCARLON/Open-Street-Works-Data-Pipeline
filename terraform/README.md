# Terraform ECS Configuration

This Terraform configuration sets up an AWS ECS (Elastic Container Service) environment with Fargate. It includes IAM roles, task definitions, security groups, and CloudWatch logging configuration.

## Components

### 1. IAM Roles and Policies

#### ECS Task Execution Role
- Creates an execution role for ECS tasks
- Includes permissions for:
  - Secrets Manager access
  - KMS decryption
  - CloudWatch Logs management
- Attaches AWS managed ECS task execution policy

#### ECS Task Role
- Creates a task role for running containers
- Includes specific permissions for:
  - Accessing Secrets Manager for specified secrets
- Custom policy for accessing specific secret ARNs

### 2. ECS Cluster
- Creates a new ECS cluster with specified name

### 3. CloudWatch Logs
- Sets up a log group for ECS tasks
- 7-day log retention period
- Log group path: `/ecs/${project_name}`

### 4. Task Definition
- Fargate-compatible task definition
- Resource allocation:
  - CPU: 2048 units
  - Memory: 4096 MB
- Network mode: awsvpc
- Container configuration includes:
  - CloudWatch logging
  - Secret environment variables:
    - MOTHERDUCK_TOKEN
    - MOTHERDB

### 5. Security Groups
- Creates a security group for ECS tasks
- Allows all outbound traffic
- Uses default VPC and subnets

## Prerequisites

- AWS credentials configured
- Default VPC available
- Required variables set in tfvars file

## Required Variables

```hcl
variable "aws_region" {}
variable "project_name" {}
variable "ecs_cluster_name" {}
variable "task_definition_name" {}
variable "container_name" {}
variable "docker_image" {}
variable "secret_name" {}
```

## Network Configuration

The configuration uses:
- Default VPC
- Default subnets
- Basic security group with outbound access

## Security Considerations

- Uses AWS Secrets Manager for sensitive data
- Implements principle of least privilege for IAM roles
- Task execution role has limited permissions
- Task role has specific secret access permissions

## Usage

1. Create a `terraform.tfvars` file with required variables
2. Initialize Terraform:
   ```bash
   terraform init
   ```
3. Review planned changes:
   ```bash
   terraform plan
   ```
4. Apply the configuration:
   ```bash
   terraform apply
   ```

## Best Practices Implemented

1. Separate roles for task execution and task running
2. CloudWatch logging enabled
3. Secrets management through AWS Secrets Manager
4. Network isolation through security groups
5. Use of Fargate for serverless container management

## Notes

- This configuration assumes the existence of secrets in AWS Secrets Manager
- Uses default VPC and subnet configuration
- Container definition includes stop timeout of 15 seconds
- All resources are tagged with project name for better resource management

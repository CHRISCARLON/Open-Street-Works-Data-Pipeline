# DEFINE PROVIDER AND AWS_REGION USE TFVARS FILE
provider "aws" {
  region = var.aws_region
}

#### 1
#### DEFINE ECS EXCECUTION ROLE, ECS EXECUTION POLICIES, ASSIGN POLICIES TO ROLE
####
resource "aws_iam_role" "ecs_task_execution_role" {
  name = "ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Principal = { Service = "ecs-tasks.amazonaws.com" }
      }
    ]
  })
}

resource "aws_iam_policy" "ecs_task_execution_policy" {
  name = "ecs_task_execution_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "secretsmanager:GetSecretValue",
          "kms:Decrypt",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_policy_attachment" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
  role       = aws_iam_role.ecs_task_execution_role.name
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_secrets_policy_attachment" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = aws_iam_policy.ecs_task_execution_policy.arn
}


#### 2
#### DEFINE ECS TASK ROLE, ECS TASK POLICIES, ASSIGN POLICIES TO ROLE
####
resource "aws_iam_role" "ecs_task_role" {
  name = "ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action    = "sts:AssumeRole",
        Effect    = "Allow",
        Principal = { Service = "ecs-tasks.amazonaws.com" }
      }
    ]
  })
}

resource "aws_iam_policy" "ecs_task_secrets_policy" {
  name   = "ecs_task_secrets_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = "secretsmanager:GetSecretValue",
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.secret_name}-*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_secrets_policy_attachment" {
  role       = aws_iam_role.ecs_task_role.name
  policy_arn = aws_iam_policy.ecs_task_secrets_policy.arn
}

#### 3
#### DEFINE ECS CLUSTER
####

resource "aws_ecs_cluster" "main" {
  name = var.ecs_cluster_name
}

#### 4
#### DEFINE CLOUDWATCH LOGS
####

resource "aws_cloudwatch_log_group" "ecs_log_group" {
  name              = "/ecs/my-ecs-task"
  retention_in_days = 7
}

#### 5
#### DEFINE ECS TASK/CONTAINER CONFIG SETTINGS - MAKE SURE TO INCLUDE THE EXECUTION AND TASK ROLES HERE
####

resource "aws_ecs_task_definition" "task_definition" {
  family                   = var.task_definition_name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  execution_role_arn       = aws_iam_role.ecs_task_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name         = var.container_name
      image        = var.docker_image
      stopTimeout  = 15
      essential    = true
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.ecs_log_group.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }
      secrets = [
        {
          name      = "MOTHERDUCK_TOKEN"
          valueFrom = "${data.aws_secretsmanager_secret.pipes.arn}:motherduck_token::"
        },
        {
          name      = "MOTHERDB"
          valueFrom = "${data.aws_secretsmanager_secret.pipes.arn}:motherdb::"
        }
      ]
    }
  ])
}

#### 6
#### DEFINE DEFAULT SECURITY GROUPINGS HERE
####


resource "aws_security_group" "default" {
  name        = "ecs-task-sg"
  description = "Security group for ECS task"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}


#### 7
#### CALL AWS PROFILE AND PULL SECRETS IN AS ENV VARIABLES
####

data "aws_secretsmanager_secret" "pipes" {
  name = var.secret_name
}

data "aws_caller_identity" "current" {}

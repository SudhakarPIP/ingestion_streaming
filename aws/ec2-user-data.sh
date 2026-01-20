#!/bin/bash
# EC2 User Data Script for deploying ingestion-streaming service

# Update system
yum update -y

# Install Docker
yum install -y docker
systemctl start docker
systemctl enable docker
usermod -a -G docker ec2-user

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Configure AWS CLI (if not already configured)
# aws configure set region us-east-1

# Login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com

# Pull and run the application
docker pull ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/ingestion-streaming:latest

# Create docker-compose.yml
cat > /home/ec2-user/docker-compose.yml <<EOF
version: '3.8'
services:
  ingestion-streaming:
    image: ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/ingestion-streaming:latest
    container_name: ingestion-streaming
    ports:
      - "8080:8080"
    environment:
      - SPRING_PROFILES_ACTIVE=dev
      - WORKER_INSTANCE_ID=ec2-instance
    healthcheck:
      test: ["CMD", "wget", "--no-verbose", "--tries=1", "--spider", "http://localhost:8080/actuator/health"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 60s
    restart: unless-stopped
EOF

# Run the service
cd /home/ec2-user
docker-compose up -d

# Wait for health check
sleep 60
for i in {1..30}; do
  if curl -f http://localhost:8080/actuator/health; then
    echo "Service is healthy!"
    exit 0
  fi
  echo "Waiting for service to become healthy... ($i/30)"
  sleep 10
done

echo "Service health check failed"
exit 1


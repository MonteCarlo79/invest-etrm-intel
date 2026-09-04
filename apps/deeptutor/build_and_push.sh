#!/bin/bash
# Usage: ./build_and_push.sh v2
set -e
TAG=${1:-v1}
ECR="319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/deeptutor-nginx"

aws ecr get-login-password --region ap-southeast-1 | \
  docker login --username AWS --password-stdin \
  319383842493.dkr.ecr.ap-southeast-1.amazonaws.com

docker build --platform linux/amd64 -t ${ECR}:${TAG} .
docker push ${ECR}:${TAG}

echo "Pushed ${ECR}:${TAG}"
echo "Update terraform.tfvars: image_deeptutor_nginx = \"${ECR}:${TAG}\""

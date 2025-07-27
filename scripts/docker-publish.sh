#!/bin/bash
GIT_COMMIT=$(git rev-parse --short HEAD)
IMAGE=jonhkr/rb2025
docker build --platform linux/amd64,linux/arm64 -t $IMAGE:$GIT_COMMIT --output=type=registry .
docker image tag $IMAGE:$GIT_COMMIT $IMAGE:latest
docker push $IMAGE:latest
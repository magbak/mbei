#!/bin/bash
set -e

LOCATION="europe-west3"
PROJECT_ID="mbei-test"
REPOSITORY_NAME="mbeirepo"

PREFIX="${LOCATION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY_NAME}"
echo $PREFIX

docker build --tag ${PREFIX}/mbei:latest .
docker build --build-arg TESTDATA_PRODUCER_TAG=${PREFIX}/mbei:latest --file gcp-perftest/performance-testing/Dockerfile \
              --tag ${PREFIX}/performance-testing:latest gcp-perftest/performance-testing

docker push ${PREFIX}/mbei:latest
docker push ${PREFIX}/performance-testing:latest

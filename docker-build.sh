#!/bin/bash

docker build --no-cache \
    --build-arg from=python:3.7-slim \
    -t siderpy_tests_3.7 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=python:3.8-slim \
    -t siderpy_tests_3.8 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.7 \
    -t siderpy_tests_hiredis_3.7 \
    -f tests/Dockerfile.hiredis .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.8 \
    -t siderpy_tests_hiredis_3.8 \
    -f tests/Dockerfile.hiredis .

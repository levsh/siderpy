#!/bin/bash

docker build --no-cache \
    --build-arg from=python:3.8-slim \
    -t siderpy_tests_3.8 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=python:3.9-slim \
    -t siderpy_tests_3.9 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=python:3.10-slim \
    -t siderpy_tests_3.10 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=python:3.11-slim \
    -t siderpy_tests_3.11 \
    -f tests/Dockerfile .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.8 \
    -t siderpy_tests_hiredis_3.8 \
    -f tests/Dockerfile.hiredis .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.9 \
    -t siderpy_tests_hiredis_3.9 \
    -f tests/Dockerfile.hiredis .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.10 \
    -t siderpy_tests_hiredis_3.10 \
    -f tests/Dockerfile.hiredis .
docker build --no-cache \
    --build-arg from=siderpy_tests_3.11 \
    -t siderpy_tests_hiredis_3.11 \
    -f tests/Dockerfile.hiredis .

#!/bin/bash
set -e

cd /home/monteirogmb/Projects/PyPAH

docker compose \
    --env-file .env.dev \
    run --rm pypah-pipeline

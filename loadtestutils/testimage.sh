#!/usr/bin/env bash


version="1.0.0"

# if --env-file is placed after it command it gives context error
docker run --env-file test.env -it "storageaccountwriter:$version"

#!/bin/bash

docker build --pull --no-cache -t calrissian:conformance -f Dockerfile.conformance ..

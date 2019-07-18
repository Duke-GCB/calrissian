#!/bin/bash

cd ..

docker build -t calrissian:conformance -f Dockerfile.conformance .

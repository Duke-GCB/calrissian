#!/usr/bin/env bash

coverage run --source=calrissian -m nose2
coverage report

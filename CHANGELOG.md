# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.10.0] - 2021-03-31

### Added

adds an argument `--pod-env_vars <yaml_file>` that allows passing environment variables to the submitted pods at runtime.

### Changed

cwltool upgraded to current version 3.0 + all requirements

## [v0.9.0] - 2020-07-08

### Added
- Uses tenacity to retry Kubernetes API calls, designed for managed offerings where API may become unavailable during upgrades #102

[Unreleased]: <https://github.com/Duke-GCB/calrissian/compare/master...dev>
[v0.10.0]: <https://github.com/Duke-GCB/calrissian/compare/0.9.0...0.10.0>
[v0.9.0]: <https://github.com/Duke-GCB/calrissian/compare/0.8.0...0.9.0>

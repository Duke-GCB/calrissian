# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

option for ensuring all files and directories passed from a node to another in a workflow is readable by all.

### Changed

### Removed

### Fixed

## [v0.9.0] - 2020-07-08

### Added
- Uses tenacity to retry Kubernetes API calls, designed for managed offerings where API may become unavailable during upgrades #102

[Unreleased]: <https://github.com/Duke-GCB/calrissian/compare/master...dev>
[v0.9.0]: <https://github.com/Duke-GCB/calrissian/v0.8.0...v0.9.0>

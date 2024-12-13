# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v0.17.2] - 2024-05-02

### Changes

- calls `setup_kubernetes` before `_setup` to set `cudaDeviceCount` by @fabricebrito in https://github.com/Duke-GCB/calrissian/pull/170

## [v0.17.1] - 2024-05-02

### Changes

## [v0.17.0] - 2024-05-02

- broken release

### Changes

- Create codemeta.json by @fabricebrito in https://github.com/Duke-GCB/calrissian/pull/163
- Conformance 1.2.1 by @fabricebrito in https://github.com/Duke-GCB/calrissian/pull/164
- Update README.md by @fabricebrito in https://github.com/Duke-GCB/calrissian/pull/165
- adds `cudaVersionMin`, `cudaComputeCapability` in `CUDARequirement` by @fabricebrito in https://github.com/Duke-GCB/calrissian/pull/168

## [v0.16.0] - 2023-11-23

### Changes

- Updated dependencies and python up to 3.10 (PR #161)
- New builds using github actions (PR #161)
- Updated badge for build status

## [v0.15.0] - 2023-06-19

### Fixes

- #101 and 158 (PR #159)

## [v0.14.0] - 2023-06-19

### Added

- adds CWL GPU requirements supports (PR #153)

## [v0.13.0] - 2023-06-06

### Added

- adds an argument `--conf <conf_file_path>` that enables CLI arguments from a json file (PR #150)
- default configuration can be defined in `$HOME/.calrissian/default.json`

### Changed

- Updated cwltool to `3.1.20230201224320` and some other dependencies. (PR #142)

## [v0.12.0] - 2023-02-13

### Added

- adds an argument `--tool-logs-basepath <local_folder_path>` that enable the tool to fetch the pod logs by tool specified in the workflow (PR #139)
- returns proper exit code when the pod fails (PR #139)

### Changed

- contraints the pod to complete with a proper termination status or raise an exception. (PR #139)

## [v0.11.0] - 2022-11-10

### Added

- adds an argument `--pod-nodeselectors <yaml_file>` to add a node selector for computing pods
- adda `--pod_serviceaccount` arg to set pods serviceaacount 

### Changed

- cwltool upgraded to current version 3.1 + all requirements
- Fixed faulty bytes in log stream (PR #137)

## [v0.10.0] - 2021-03-31

### Added

adds an argument `--pod-env_vars <yaml_file>` that allows passing environment variables to the submitted pods at runtime.

### Changed

cwltool upgraded to current version 3.0 + all requirements

## [v0.9.0] - 2020-07-08

### Added
- Uses tenacity to retry Kubernetes API calls, designed for managed offerings where API may become unavailable during upgrades #102

[Unreleased]: <https://github.com/Duke-GCB/calrissian/compare/master...0.16.0>
[v0.16.0]: <https://github.com/Duke-GCB/calrissian/compare/0.15.0...0.16.0>
[v0.15.0]: <https://github.com/Duke-GCB/calrissian/compare/0.14.0...0.15.0>
[v0.14.0]: <https://github.com/Duke-GCB/calrissian/compare/0.13.0...0.14.0>
[v0.13.0]: <https://github.com/Duke-GCB/calrissian/compare/0.12.0...0.13.0>
[v0.12.0]: <https://github.com/Duke-GCB/calrissian/compare/0.11.0...0.12.0>
[v0.11.0]: <https://github.com/Duke-GCB/calrissian/compare/0.10.0...0.11.0>
[v0.10.0]: <https://github.com/Duke-GCB/calrissian/compare/0.9.0...0.10.0>
[v0.9.0]: <https://github.com/Duke-GCB/calrissian/compare/0.8.0...0.9.0>

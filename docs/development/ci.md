# Continuous integration and image security

Pull requests run the Python build and test matrix. They do not build or publish
container images and receive no GHCR publishing credentials. The manually
dispatched conformance workflow pulls the public test image without a registry
token and has no package-write permission.

Trusted pushes to `develop` publish `latest-dev`; pushes to `main` or `master`
publish `latest`. Version tags such as `v1.2.3` and `v1.2.3-rc1` publish the
matching normalized version tag. Each published image also receives a tag with
the full source commit SHA. Before any image build begins, this workflow reuses
the Python build and test matrix on Python 3.10 through 3.14; a failed job blocks
scanning and publishing. The runtime and conformance images are built with
Kaniko, exported as archives, scanned, and then copied to GHCR with Skopeo.
The conformance image is built from the locally staged runtime image so the
published pair comes from the same workflow run.

## Trivy policy

The runtime and conformance image archives are scanned for vulnerabilities
rated **HIGH** or **CRITICAL**. Any such finding fails the publishing job. The
CycloneDX SBOMs, image archives, and JSON scan reports are retained as workflow
artifacts for 30 days. The SBOMs are not submitted to OWASP Dependency-Check by
this workflow.

There are no vulnerability exceptions by default. If a finding is confirmed
to be a false positive or an accepted temporary risk, add a narrowly scoped
entry to the repository `.trivyignore.yaml` in a reviewed change. Include the
finding ID, package PURL or affected path where possible, a short justification,
and an `expired_at` date no more than 90 days away. Reassess or remove the
exception before it expires; do not add permanent or broad CVE suppressions.

## Conformance workflow

The `conformance` workflow runs all supported CWL suites when manually
dispatched or when a GitHub release is published. A manual run defaults to the
`latest-dev` conformance image and accepts another published image tag. Release
runs select the image corresponding to the release tag. The workflow creates a
Minikube cluster with Calico networking and persistent storage, deploys the
conformance chart, waits for each suite Job to complete successfully, and
uploads suite logs and generated badge results for 90 days. A failed suite
fails the workflow.

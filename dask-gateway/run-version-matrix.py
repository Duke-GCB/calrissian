#!/usr/bin/env python3
"""Deploy and run the real-cluster Dask suite for each configured release."""
import atexit
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from version_config import load_version_config


ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
CONFIG = load_version_config()
KUBE_CONTEXT = "calrissian-conformance"


def versions_from_environment():
    configured = os.environ.get("DASK_TEST_VERSIONS", "").strip()
    versions = [part.strip() for part in configured.split(",") if part.strip()]
    if not versions:
        versions = CONFIG["dask_releases"]
    invalid = [version for version in versions if version not in CONFIG["dask_releases"]]
    if invalid:
        raise ValueError(
            f"Dask release(s) not listed in versions.json: {', '.join(invalid)}"
        )
    if len(set(versions)) != len(versions):
        raise ValueError("Dask release list contains duplicates")
    return versions


def run_logged(command, env, log_path):
    with log_path.open("w") as log:
        log.write("$ " + " ".join(command) + "\n")
        log.flush()
        return subprocess.run(
            command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT,
            check=False,
        ).returncode


def collect_diagnostics(env, directory, version):
    context = KUBE_CONTEXT
    path = directory / f"dask-{version}-diagnostics.log"
    commands = [
        ["kubectl", "--context", context, "get", "nodes,pods,jobs,pvc", "--all-namespaces", "-o", "wide"],
        ["kubectl", "--context", context, "get", "events", "--all-namespaces", "--sort-by=.lastTimestamp"],
        ["kubectl", "--context", context, "describe", "job", "stage-conformance-test-data", "--namespace", "calrissian-conformance"],
        ["kubectl", "--context", context, "describe", "pods", "--selector", "job-name=stage-conformance-test-data", "--namespace", "calrissian-conformance"],
        ["kubectl", "--context", context, "logs", "job/stage-conformance-test-data", "--all-containers", "--prefix", "--namespace", "calrissian-conformance"],
        ["helm", "--kube-context", context, "list", "--all-namespaces", "--output", "json"],
    ]
    with path.open("w") as log:
        for command in commands:
            log.write("$ " + " ".join(command) + "\n")
            log.flush()
            result = subprocess.run(
                command, cwd=REPO, env=env, text=True, stdout=log,
                stderr=subprocess.STDOUT, check=False,
            )
            log.write(f"[exit {result.returncode}]\n\n")
    return path


def run_cleanup_commands(commands, env, log_path):
    """Run cleanup commands independently so a missing resource is harmless."""
    outcomes = []
    with log_path.open("w") as log:
        for command in commands:
            log.write("$ " + " ".join(command) + "\n")
            log.flush()
            result = subprocess.run(
                command, cwd=REPO, env=env, text=True, stdout=log,
                stderr=subprocess.STDOUT, check=False,
            )
            log.write(f"[exit {result.returncode}]\n\n")
            outcomes.append(result.returncode)
    return int(any(outcomes))


def cleanup_gateway(env, log_path):
    context = KUBE_CONTEXT
    return run_cleanup_commands([[
        "helm", "--kube-context", context, "uninstall", "dask-gateway",
        "--namespace", "dask-gateway", "--ignore-not-found", "--wait", "--timeout", "120s",
    ]], env, log_path)


def cleanup_releases(env, log_path):
    """Remove hook resources before Helm deletes the PVCs they mount."""
    context = KUBE_CONTEXT
    commands = [
        ["kubectl", "--context", context, "--namespace", "calrissian-conformance",
         "delete", "job", "stage-conformance-test-data", "--ignore-not-found", "--wait=true", "--timeout=120s"],
        ["kubectl", "--context", context, "--namespace", "calrissian-conformance",
         "delete", "pod", "inspect-volumes", "--ignore-not-found", "--wait=true", "--timeout=120s"],
        ["helm", "--kube-context", context, "uninstall", "dask-gateway",
         "--namespace", "dask-gateway", "--ignore-not-found", "--wait", "--timeout", "120s"],
        ["helm", "--kube-context", context, "uninstall", "calrissian-conformance",
         "--namespace", "calrissian-conformance", "--ignore-not-found", "--wait", "--timeout", "120s"],
    ]
    return run_cleanup_commands(commands, env, log_path)


def wait_for_claims_deleted(env, timeout=180):
    claims = ("conformance-test-data", "conformance-test-output")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = []
        for claim in claims:
            result = subprocess.run(
                ["kubectl", "--context", KUBE_CONTEXT, "--namespace",
                 "calrissian-conformance", "get", "pvc", claim, "-o", "name"],
                cwd=REPO, env=env, text=True, capture_output=True, check=False,
            )
            if result.returncode and "NotFound" not in result.stderr:
                raise RuntimeError(result.stderr.strip())
            if result.stdout.strip():
                remaining.append(claim)
        if not remaining:
            return
        time.sleep(1)
    raise TimeoutError(f"Conformance PVCs were not deleted within {timeout}s: {remaining}")


def main():
    versions = versions_from_environment()
    test_id = os.environ.get("DASK_TEST_ID", "").strip()
    if test_id and not test_id.isdigit():
        raise ValueError(f"Invalid DASK_TEST_ID: {test_id}")
    directory = ROOT / "test-results" / f"matrix-{uuid.uuid4().hex[:8]}"
    directory.mkdir(parents=True)
    outcomes = []

    initial_cleanup_log = directory / "initial-cleanup.log"
    if cleanup_releases(os.environ.copy(), initial_cleanup_log):
        raise RuntimeError(f"Could not clean existing Dask profile; see {initial_cleanup_log}")
    wait_for_claims_deleted(os.environ.copy())

    finalized = False

    def cleanup_if_interrupted():
        if not finalized:
            cleanup_releases(
                os.environ.copy(), directory / "interrupted-cleanup.log"
            )

    atexit.register(cleanup_if_interrupted)

    for version in versions:
        print(
            f"Deploying Dask {version} with Gateway client/chart "
            f"{CONFIG['gateway_client']}/{CONFIG['gateway_chart']}", flush=True,
        )
        env = os.environ.copy()
        env["DASK_VERSION"] = version
        outcome = {
            "dask": version,
            "gateway_client": CONFIG["gateway_client"],
            "gateway_chart": CONFIG["gateway_chart"],
        }
        deploy_log = directory / f"dask-{version}-deploy.log"
        test_log = directory / f"dask-{version}-test.log"
        delete_log = directory / f"dask-{version}-delete.log"

        deploy_code = run_logged(
            [sys.executable, str(ROOT / "run-deploy.py"), "--dask-version", version],
            env, deploy_log,
        )
        outcome["deploy"] = "passed" if deploy_code == 0 else "failed"
        outcome["deploy_log"] = str(deploy_log.relative_to(REPO))
        if deploy_code:
            diagnostics = collect_diagnostics(env, directory, version)
            outcome["diagnostics_log"] = str(diagnostics.relative_to(REPO))

        test_code = None
        try:
            if deploy_code == 0:
                test_env = env.copy()
                test_command = ["task", "dask:test"]
                if test_id:
                    test_command.append(f"ID={test_id}")
                test_code = run_logged(test_command, test_env, test_log)
                outcome["test"] = "passed" if test_code == 0 else "failed"
                outcome["test_log"] = str(test_log.relative_to(REPO))
                if test_code:
                    diagnostics = collect_diagnostics(env, directory, version)
                    outcome["diagnostics_log"] = str(diagnostics.relative_to(REPO))
            else:
                outcome["test"] = "not-run"
        finally:
            # Keep conformance volumes mounted and populated between Dask releases.
            delete_code = cleanup_gateway(env, delete_log)
            outcome["cleanup"] = "passed" if delete_code == 0 else "failed"
            outcome["delete_log"] = str(delete_log.relative_to(REPO))

        outcomes.append(outcome)
        print(json.dumps(outcome), flush=True)

    results_path = directory / "results.json"
    results_path.write_text(json.dumps({
        "gateway_chart": CONFIG["gateway_chart"],
        "gateway_client": CONFIG["gateway_client"],
        "results": outcomes,
    }, indent=2) + "\n")
    print(f"Matrix results: {results_path}", flush=True)
    final_cleanup_log = directory / "final-cleanup.log"
    cleanup_code = cleanup_releases(os.environ.copy(), final_cleanup_log)
    try:
        wait_for_claims_deleted(os.environ.copy())
    except Exception as error:
        print(f"Final PVC cleanup failed: {error}", file=sys.stderr)
        cleanup_code = 1
    finalized = True
    atexit.unregister(cleanup_if_interrupted)
    results_path.write_text(json.dumps({
        "gateway_chart": CONFIG["gateway_chart"],
        "gateway_client": CONFIG["gateway_client"],
        "results": outcomes,
        "final_cleanup": "passed" if cleanup_code == 0 else "failed",
        "final_cleanup_log": str(final_cleanup_log.relative_to(REPO)),
    }, indent=2) + "\n")
    return int(cleanup_code or any(
        item["deploy"] != "passed"
        or item["test"] != "passed"
        or item["cleanup"] != "passed"
        for item in outcomes
    ))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as error:
        raise SystemExit(str(error)) from error

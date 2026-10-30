#!/usr/bin/env python3
"""Deploy the Dask profile using versions.json as its source of truth."""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from version_config import load_version_config


ROOT = Path(__file__).resolve().parent
KUBE_CONTEXT = "calrissian-conformance"


def main():
    config = load_version_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dask-version",
        default=os.environ.get("DASK_VERSION") or config["dask_releases"][0],
        choices=config["dask_releases"],
        help="Dask and Distributed release to test (defaults to the first configured release)",
    )
    args = parser.parse_args()

    env = os.environ.copy()
    env.update({
        "DASK_VERSION": args.dask_version,
        "DASK_GATEWAY_CLIENT_VERSION": config["gateway_client"],
        "DASK_GATEWAY_CHART_VERSION": config["gateway_chart"],
        "DASK_WORKER_TAG": (
            f"dask-{args.dask_version}-gateway-client-{config['gateway_client']}"
            f"-chart-{config['gateway_chart']}"
        ),
    })
    deploy = subprocess.run(
        ["skaffold", "run", "--profile", "dask"], cwd=ROOT.parent, env=env,
        check=False,
    )
    if deploy.returncode:
        return deploy.returncode

    context = KUBE_CONTEXT
    release = subprocess.run(
        ["helm", "--kube-context", context, "--namespace", "dask-gateway",
         "list", "--output", "json"],
        cwd=ROOT.parent, env=env, text=True, capture_output=True, check=False,
    )
    if release.returncode:
        print("Could not verify the deployed Dask Gateway Helm release:", file=sys.stderr)
        print(release.stderr, file=sys.stderr)
        return release.returncode
    releases = json.loads(release.stdout)
    installed = next((item for item in releases if item.get("name") == "dask-gateway"), None)
    actual_chart = installed.get("chart", "") if installed else ""
    expected_chart = f"dask-gateway-{config['gateway_chart']}"
    if actual_chart != expected_chart:
        print(
            f"Deployment version check failed: context {context!r} has chart "
            f"{actual_chart or '(not installed)'}, expected {expected_chart}.",
            file=sys.stderr,
        )
        return 1
    print(
        f"Verified deployment: context={context}, Dask={args.dask_version}, "
        f"Gateway client={config['gateway_client']}, chart={config['gateway_chart']}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

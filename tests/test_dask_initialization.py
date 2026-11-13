"""Run the shipped lifecycle script in isolated Python without Calrissian."""

import json
import subprocess
import sys
from importlib.resources import files
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    ("mounts", "profile_support"), [(None, False), ("[]", True), ("descriptors", True)]
)
def test_initializer_runs_without_runner_packages(
    tmp_path: Path, mounts: str | None, profile_support: bool
) -> None:
    encoded = None
    if mounts is not None:
        encoded = json.dumps(
            [
                {
                    "claimName": "execution",
                    "mountPath": "/task/input",
                    "subPath": "source",
                    "readOnly": True,
                }
            ]
            if mounts == "descriptors"
            else []
        )
    completed = run_initializer(tmp_path, encoded, profile_support)
    assert completed.returncode == 0, completed.stderr
    observed = json.loads((tmp_path / "observations.json").read_text())
    assert observed["workers"] == 2
    assert observed["options"]["image"] == "occurrence:test"
    assert observed["options"]["worker_memory"] == "2G"
    if encoded is not None:
        assert observed["options"]["worker_pvc_mounts"] == encoded
    else:
        assert "worker_pvc_mounts" not in observed["options"]
    assert (tmp_path / "cluster-name").read_text() == "calrissian-owned-cluster"


def test_initializer_rejects_unsupported_profile_before_creating_cluster(
    tmp_path: Path,
) -> None:
    completed = run_initializer(tmp_path, "[]", profile_support=False)
    assert completed.returncode != 0
    assert "Gateway profile does not support task PVC propagation" in completed.stderr
    assert not (tmp_path / "observations.json").exists()
    assert not (tmp_path / "cluster-name").exists()


def run_initializer(
    directory: Path, mounts: str | None, profile_support: bool
) -> subprocess.CompletedProcess[str]:
    """Execute the unmodified shipped script with only a stdlib Gateway stub.

    Isolated mode and disabled site initialization exclude installed Calrissian
    packages. The explicit path contains only the script and Gateway stub.
    """
    script = directory / "init-dask.py"
    script.write_bytes(
        files("calrissian_dask.resources").joinpath("init-dask.py").read_bytes()
    )
    stub = Path(__file__).parent / "fixtures/dask_gateway_stub.py"
    (directory / "dask_gateway.py").write_bytes(stub.read_bytes())
    environment = {
        "DASK_CLUSTER_NAME_PATH": str(directory / "cluster-name"),
        "DASK_GATEWAY_URL": "http://gateway.test",
        "DASK_GATEWAY_IMAGE": "occurrence:test",
        "DASK_GATEWAY_WORKER_CORES": "1",
        "DASK_GATEWAY_WORKER_CORES_LIMIT": "1",
        "DASK_GATEWAY_WORKER_MEMORY": "2G",
        "DASK_GATEWAY_CLUSTER_MAX_CORES": "2",
        "DASK_GATEWAY_CLUSTER_MAX_RAM": "4G",
        "STUB_SUPPORTS_PVC": "1" if profile_support else "0",
        "STUB_OBSERVATIONS": str(directory / "observations.json"),
    }
    if mounts is not None:
        environment["DASK_GATEWAY_PVC_MOUNTS"] = mounts
    bootstrap = (
        "import runpy, sys; sys.path.insert(0, sys.argv[1]); "
        "runpy.run_path(sys.argv[2], run_name='__main__')"
    )
    return subprocess.run(
        [sys.executable, "-I", "-S", "-c", bootstrap, str(directory), str(script)],
        env=environment,
        cwd=directory,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )

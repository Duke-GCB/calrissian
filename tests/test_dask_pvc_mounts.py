"""Test opt-in task-to-worker PVC mappings and Gateway profile validation."""

import json
from argparse import ArgumentParser
from unittest.mock import patch

import pytest
from calrissian.context import CalrissianRuntimeContext
from calrissian_dask.plugin import DaskGatewayExtension
from calrissian_dask.pvc_mounts import serialize_task_pvcs, worker_pvc_config
from jsonschema import ValidationError


def test_flag_defaults_off_and_is_forwarded() -> None:
    extension = DaskGatewayExtension()
    parser = ArgumentParser()
    extension.register_arguments(parser)
    for arguments, expected in [([], False), (["--dask-mount-task-pvcs"], True)]:
        runtime = CalrissianRuntimeContext()
        extension.configure_runtime(parser.parse_args(arguments), runtime)
        assert runtime.extension_options["dask_gateway"]["mount_task_pvcs"] is expected


def test_only_pvcs_are_propagated_with_resolved_paths_and_readonly() -> None:
    volumes = [
        {"name": "data", "persistentVolumeClaim": {"claimName": "execution"}},
        {"name": "credentials", "secret": {"secretName": "credentials"}},
        {"name": "coordination", "emptyDir": {}},
    ]
    mounts: list[dict[str, object]] = [
        {
            "name": "data",
            "mountPath": "/task/input",
            "subPath": "first/output",
            "readOnly": True,
        },
        {"name": "data", "mountPath": "/task/out", "subPath": "second/output"},
        {"name": "credentials", "mountPath": "/credentials"},
        {"name": "coordination", "mountPath": "/shared"},
    ]
    encoded = serialize_task_pvcs(volumes, mounts)
    original = {"env": [{"name": "KEEP", "value": "yes"}]}
    configuration = worker_pvc_config(encoded, ["execution"], {}, original)
    propagated = configuration["worker_extra_container_config"]
    assert isinstance(propagated, dict)
    assert propagated["env"] == original["env"]
    assert [mount["mountPath"] for mount in propagated["volumeMounts"]] == [
        "/task/input",
        "/task/out",
    ]
    assert [mount["subPath"] for mount in propagated["volumeMounts"]] == [
        "first/output",
        "second/output",
    ]
    assert [mount["readOnly"] for mount in propagated["volumeMounts"]] == [True, False]
    assert "volumeMounts" not in original
    pod = configuration["worker_extra_pod_config"]
    assert isinstance(pod, dict)
    assert len(pod["volumes"]) == 1
    assert pod["volumes"][0]["persistentVolumeClaim"]["readOnly"] is False
    assert len({mount["name"] for mount in propagated["volumeMounts"]}) == 1


@pytest.mark.parametrize("invalid", ["claim", "path", "subpath", "collision"])
def test_gateway_rejects_invalid_mounts(invalid: str) -> None:
    mount = {
        "claimName": "execution",
        "mountPath": "/task",
        "subPath": "",
        "readOnly": False,
    }
    existing = {}
    if invalid == "claim":
        mount["claimName"] = "unapproved"
    elif invalid == "path":
        mount["mountPath"] = "/task/../etc"
    elif invalid == "subpath":
        mount["subPath"] = "../other"
    else:
        existing = {"volumeMounts": [{"name": "other", "mountPath": "/task"}]}
    with pytest.raises(ValueError):
        worker_pvc_config(json.dumps([mount]), ["execution"], {}, existing)


def test_claim_readonly_and_subpath_expressions() -> None:
    volumes = [
        {
            "name": "data",
            "persistentVolumeClaim": {"claimName": "execution", "readOnly": True},
        }
    ]
    mounts = [{"name": "data", "mountPath": "/data"}]
    assert json.loads(serialize_task_pvcs(volumes, mounts))[0]["readOnly"] is True
    mounts[0]["subPathExpr"] = "$(POD_NAME)"
    with pytest.raises(ValueError, match="resolved subPath"):
        serialize_task_pvcs(volumes, mounts)


def test_bad_wire_descriptor_fails_validation() -> None:
    with pytest.raises(ValidationError):
        worker_pvc_config('[{"claimName":"execution"}]', ["execution"], {}, {})


def test_descriptor_reaches_dask_initialization_environment() -> None:
    from calrissian_dask.dask import KubernetesDaskPodBuilder

    with (
        patch("calrissian.job.KubernetesPodBuilder.__init__", return_value=None),
        patch.object(KubernetesDaskPodBuilder, "requirements", [], create=True),
    ):
        builder = KubernetesDaskPodBuilder("gateway", False, pvc_mounts="[]")
    builder.container_image = "probe:local"
    builder.environment = {}
    builder.dask_requirement = {}
    environment = builder.container_environment()
    assert {"name": "DASK_GATEWAY_PVC_MOUNTS", "value": "[]"} in environment


def test_existing_pvc_volume_is_reused_without_escalating_profile_access() -> None:
    descriptors = json.dumps(
        [
            {
                "claimName": "execution",
                "mountPath": "/input",
                "subPath": "input",
                "readOnly": True,
            },
            {
                "claimName": "execution",
                "mountPath": "/out",
                "subPath": "out",
                "readOnly": False,
            },
        ]
    )
    base = {
        "volumes": [
            {"name": "existing", "persistentVolumeClaim": {"claimName": "execution"}}
        ]
    }
    result = worker_pvc_config(descriptors, ["execution"], base, {})
    pod = result["worker_extra_pod_config"]
    container = result["worker_extra_container_config"]
    assert isinstance(pod, dict) and isinstance(container, dict)
    assert len(pod["volumes"]) == 1
    assert {mount["name"] for mount in container["volumeMounts"]} == {"existing"}
    readonly_base = {
        "volumes": [
            {
                "name": "existing",
                "persistentVolumeClaim": {"claimName": "execution", "readOnly": True},
            }
        ]
    }
    with pytest.raises(ValueError, match="read-only access"):
        worker_pvc_config(descriptors, ["execution"], readonly_base, {})

"""Propagate a task's resolved PVC mounts without exposing other pod volumes."""

import copy
import json
from collections.abc import Mapping, Sequence
from importlib.resources import files
from pathlib import PurePosixPath

from jsonschema import Draft202012Validator

MOUNTS_ENVIRONMENT = "DASK_GATEWAY_PVC_MOUNTS"
MOUNTS_OPTION = "worker_pvc_mounts"


def _validate(
    value: str, schema: Mapping[str, object] | None = None
) -> list[dict[str, object]]:
    schema = (
        schema
        if schema is not None
        else json.loads(
            files("calrissian_dask.resources")
            .joinpath("pvc-mounts.schema.json")
            .read_text()
        )
    )
    document = json.loads(value)
    Draft202012Validator(schema).validate(document)
    return document


def serialize_task_pvcs(
    volumes: Sequence[Mapping[str, object]], mounts: Sequence[Mapping[str, object]]
) -> str:
    """Serialize only resolved PVC-backed task mounts, retaining restrictions.

    Secrets, ConfigMaps and emptyDir volumes are excluded. Unsupported subPath
    expressions fail rather than silently mounting a different directory.
    """
    claims = {
        volume["name"]: volume["persistentVolumeClaim"]
        for volume in volumes
        if "persistentVolumeClaim" in volume
    }
    selected = []
    for mount in mounts:
        claim = claims.get(mount["name"])
        if not isinstance(claim, Mapping):
            continue
        if mount.get("subPathExpr"):
            raise ValueError("Dask PVC propagation requires a resolved subPath")
        selected.append(
            {
                "claimName": claim["claimName"],
                "mountPath": mount["mountPath"],
                "subPath": mount.get("subPath") or "",
                "readOnly": bool(claim.get("readOnly") or mount.get("readOnly")),
            }
        )
    encoded = json.dumps(selected, sort_keys=True)
    _validate(encoded)
    return encoded


def _entries(configuration: Mapping[str, object], key: str) -> list[dict[str, object]]:
    entries = configuration.get(key, [])
    if not isinstance(entries, list) or not all(
        isinstance(entry, dict) for entry in entries
    ):
        raise ValueError(f"Invalid Kubernetes configuration: {key}")
    return entries


def _pvc_volume_names(
    descriptors: list[dict[str, object]], volumes: list[dict[str, object]]
) -> dict[str, str]:
    """Use one volume per claim; per-path restrictions belong on volumeMounts."""
    requested = {str(descriptor["claimName"]) for descriptor in descriptors}
    existing_readonly: dict[str, bool] = {}
    names: dict[str, str] = {}
    used_names = {str(volume["name"]) for volume in volumes}
    for volume in volumes:
        source = volume.get("persistentVolumeClaim")
        if isinstance(source, dict) and source.get("claimName") in requested:
            claim = str(source["claimName"])
            if claim in names:
                raise ValueError("Gateway profile contains duplicate volumes for a PVC")
            names[claim] = str(volume["name"])
            existing_readonly[claim] = bool(source.get("readOnly", False))
    for claim in sorted(requested):
        readonly = all(
            descriptor["readOnly"]
            for descriptor in descriptors
            if descriptor["claimName"] == claim
        )
        if claim in names:
            if existing_readonly[claim] and not readonly:
                raise ValueError(
                    "Gateway profile grants only read-only access to this PVC"
                )
            continue
        name = f"calrissian-pvc-{len(volumes)}"
        while name in used_names:
            name += "-next"
        volumes.append(
            {
                "name": name,
                "persistentVolumeClaim": {"claimName": claim, "readOnly": readonly},
            }
        )
        names[claim] = name
        used_names.add(name)
    return names


def worker_pvc_config(
    value: str,
    allowed_claims: Sequence[str],
    pod_config: Mapping[str, object],
    container_config: Mapping[str, object],
    *,
    schema: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build Gateway worker overrides for explicitly operator-approved PVCs.

    Existing configuration is preserved. Unknown claims, ambiguous paths and
    mount collisions fail before cluster creation. No scheduler mount is added.
    """
    descriptors = _validate(value, schema)
    pod = copy.deepcopy(dict(pod_config))
    container = copy.deepcopy(dict(container_config))
    volumes = _entries(pod, "volumes")
    mounts = _entries(container, "volumeMounts")
    paths = {mount["mountPath"] for mount in mounts}
    if any(descriptor["claimName"] not in allowed_claims for descriptor in descriptors):
        raise ValueError("PVC is not approved for this Gateway profile")
    names = _pvc_volume_names(descriptors, volumes)
    for descriptor in descriptors:
        mount_path = descriptor["mountPath"]
        sub_path = descriptor["subPath"]
        if not isinstance(mount_path, str) or not isinstance(sub_path, str):
            raise TypeError("PVC mount paths must be strings")
        target = PurePosixPath(mount_path)
        source = PurePosixPath(sub_path)
        if not target.is_absolute() or ".." in target.parts:
            raise ValueError("Worker PVC mountPath must be absolute and unambiguous")
        if source.is_absolute() or ".." in source.parts:
            raise ValueError("Worker PVC subPath must be relative and unambiguous")
        if str(target) in paths:
            raise ValueError("Worker PVC mountPath conflicts with another mount")
        name = names[str(descriptor["claimName"])]
        mounts.append(
            {
                "name": name,
                "mountPath": str(target),
                "subPath": descriptor["subPath"],
                "readOnly": descriptor["readOnly"],
            }
        )
        paths.add(str(target))
    pod["volumes"] = volumes
    container["volumeMounts"] = mounts
    return {"worker_extra_pod_config": pod, "worker_extra_container_config": container}

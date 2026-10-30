"""Load and validate the Dask integration version contract."""
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parent
VERSION_PATTERN = re.compile(r"\d{4}\.\d{1,2}\.\d+")


def load_version_config():
    config = json.loads((ROOT / "versions.json").read_text())
    for key in ("gateway_chart", "gateway_client"):
        value = config.get(key)
        if not isinstance(value, str) or not VERSION_PATTERN.fullmatch(value):
            raise ValueError(f"versions.json must define a valid {key} version")

    versions = config.get("dask_releases")
    if (not isinstance(versions, list) or not versions
            or any(not isinstance(v, str) or not VERSION_PATTERN.fullmatch(v) for v in versions)):
        raise ValueError("versions.json dask_releases must be a non-empty list of versions")
    if len(set(versions)) != len(versions):
        raise ValueError("versions.json dask_releases must not contain duplicates")
    return config


def validate_deployed_versions(config, dask, distributed, gateway_client, gateway_chart):
    actual = {
        "dask": dask,
        "distributed": distributed,
        "gateway_client": gateway_client,
        "gateway_chart": gateway_chart,
    }
    errors = []
    if dask not in config["dask_releases"]:
        errors.append(f"Dask {dask} is not listed in versions.json dask_releases")
    if distributed != dask:
        errors.append(f"Distributed {distributed} does not match Dask {dask}")
    if gateway_client != config["gateway_client"]:
        errors.append(
            f"Gateway client {gateway_client} does not match versions.json "
            f"gateway_client {config['gateway_client']}"
        )
    if gateway_chart != config["gateway_chart"]:
        errors.append(
            f"Gateway chart {gateway_chart} does not match versions.json "
            f"gateway_chart {config['gateway_chart']}"
        )
    return actual, errors

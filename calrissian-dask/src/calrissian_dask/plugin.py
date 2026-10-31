"""Calrissian entry point for the Dask Gateway extension."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from importlib.resources import files
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from calrissian.context import CalrissianRuntimeContext

import cwltool
import yaml
from calrissian.extensions import EXTENSION_API_VERSION
from cwltool.process import get_schema, use_custom_schema

from calrissian_dask.dask import (
    CalrissianCommandLineDaskJob,
    DaskPodMonitor,
    dask_req_validate,
)

REQUIREMENT = "https://calrissian-cwl.github.io/schema#DaskGatewayRequirement"


def register_dask_schema():
    extension_schemas = {
        "v1.0": "extensions.yml",
        "v1.1": "extensions-v1.1.yml",
        "v1.2": "extensions-v1.2.yml",
    }
    dask_schema = yaml.safe_load(
        files("calrissian_dask.resources").joinpath("schema.yaml").read_text("utf-8")
    )

    for version, extension_file in extension_schemas.items():
        schema = yaml.safe_load(
            files("cwltool").joinpath(extension_file).read_text("utf-8")
        )
        for definition in dask_schema["$graph"]:
            if "name" in definition:
                schema["$graph"].append(
                    dict(definition, name=dask_schema["$base"] + definition["name"])
                )
        use_custom_schema(
            version, "http://commonwl.org/cwltool", yaml.safe_dump(schema)
        )
        get_schema(version)

    if REQUIREMENT not in cwltool.process.supportedProcessRequirements:
        cwltool.process.supportedProcessRequirements.append(REQUIREMENT)


class DaskGatewayExtension:
    """Plugin hooks that register and run the Dask Gateway CWL extension."""

    api_version = EXTENSION_API_VERSION
    logger_names = ("calrissian.dask",)

    def register_arguments(self, parser: ArgumentParser) -> None:
        """Register Gateway lifecycle and optional worker PVC settings."""
        parser.add_argument(
            "--dask-gateway-url",
            type=str,
            nargs="?",
            help="Defines the Dask Gateway URL",
            required=False,
        )
        parser.add_argument(
            "--dask-script-configmap",
            type=str,
            nargs="?",
            help="Name of the already existing ConfigMap with custom Dask scripts",
            required=False,
        )

        parser.add_argument(
            "--dask-mount-task-pvcs",
            action="store_true",
            help="Mirror this task's resolved PVC mounts onto Gateway workers; requires a compatible Gateway profile",
        )

    def configure_runtime(
        self, args: Namespace, runtime_context: CalrissianRuntimeContext
    ) -> None:
        """Store operator settings without changing the application CWL."""
        runtime_context.extension_options["dask_gateway"] = {
            "mount_task_pvcs": getattr(args, "dask_mount_task_pvcs", False),
            "url": args.dask_gateway_url,
            "script_configmap": args.dask_script_configmap,
        }

    def custom_schema_callback(self, args):
        return register_dask_schema if args.dask_gateway_url else None

    def make_job_runner(self, tool, runtime_context):
        requirement, _ = tool.get_requirement(REQUIREMENT)
        if dask_req_validate(requirement):
            return CalrissianCommandLineDaskJob
        return None

    def cleanup(self, args, runtime_context):
        if args and args.dask_gateway_url:
            DaskPodMonitor.cleanup()
            return True
        return False


def create_extension():
    return DaskGatewayExtension()

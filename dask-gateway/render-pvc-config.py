"""Render an operator Gateway helper without installing Calrissian on the server."""

import argparse
from pathlib import Path


def main() -> None:
    """Emit schema validation and worker overrides for explicitly allowed claims."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-claim", action="append", required=True)
    arguments = parser.parse_args()
    extension = (
        Path(__file__).resolve().parents[1] / "calrissian-dask/src/calrissian_dask"
    )
    schema = (extension / "resources/pvc-mounts.schema.json").read_text()
    print((extension / "pvc_mounts.py").read_text())
    print("_CALRISSIAN_PVC_SCHEMA = json.loads(" + repr(schema) + ")")
    print("_CALRISSIAN_ALLOWED_CLAIMS = " + repr(arguments.allow_claim))
    print("""
def calrissian_worker_pvcs(options):
    return worker_pvc_config(
        options.worker_pvc_mounts, _CALRISSIAN_ALLOWED_CLAIMS,
        get_property("gateway.backend.worker.extraPodConfig", {}),
        get_property("gateway.backend.worker.extraContainerConfig", {}),
        schema=_CALRISSIAN_PVC_SCHEMA,
    )
""")


if __name__ == "__main__":
    main()

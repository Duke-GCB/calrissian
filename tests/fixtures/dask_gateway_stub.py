"""Record standalone initializer calls without external packages or networking."""

import json
import os
from pathlib import Path


class Cluster:
    """Record allocation requests for the initializer's synthetic cluster."""

    name = "calrissian-owned-cluster"

    def __init__(self, options: dict[str, object]) -> None:
        self.options = options

    def scale(self, workers: int) -> None:
        """Persist the requested allocation for the parent test to inspect."""
        Path(os.environ["STUB_OBSERVATIONS"]).write_text(
            json.dumps({"options": self.options, "workers": workers})
        )


class Gateway:
    """Supply the Gateway methods exercised by the standalone initializer."""

    def __init__(self, address: str) -> None:
        if address != "http://gateway.test":
            raise ValueError("Unexpected test Gateway address")

    def cluster_options(self) -> dict[str, object]:
        """Advertise PVC support only when the selected test requests it."""
        if os.environ["STUB_SUPPORTS_PVC"] == "1":
            return {"worker_pvc_mounts": "[]"}
        return {}

    def new_cluster(
        self, options: dict[str, object], shutdown_on_close: bool
    ) -> Cluster:
        """Create a synthetic cluster without transferring disposal ownership."""
        if shutdown_on_close:
            raise ValueError("Calrissian must retain disposal ownership")
        return Cluster(options)

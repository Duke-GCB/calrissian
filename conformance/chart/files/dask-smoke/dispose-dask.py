from pathlib import Path
import resource
import runpy
try:
    runpy.run_path(str(Path(__file__).with_name("dispose-dask-default.py")), run_name="__main__")
finally:
    # Linux reports ru_maxrss in KiB. Includes imports and Gateway shutdown.
    print(f"cleanup peak RSS KiB: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}", flush=True)

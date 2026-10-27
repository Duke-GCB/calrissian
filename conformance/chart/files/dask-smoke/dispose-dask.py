from pathlib import Path
import calrissian
import resource
import runpy
try:
    runpy.run_path(str(Path(calrissian.__file__).parent / "dask" / "dispose-dask.py"), run_name="__main__")
finally:
    # Linux reports ru_maxrss in KiB. Includes imports and Gateway shutdown.
    print(f"cleanup peak RSS KiB: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}", flush=True)

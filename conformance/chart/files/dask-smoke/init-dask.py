from pathlib import Path
import runpy
import traceback
try:
    runpy.run_path(str(Path(__file__).with_name("init-dask-default.py")), run_name="__main__")
except BaseException:
    Path("init-error.txt").write_text(traceback.format_exc())
    raise

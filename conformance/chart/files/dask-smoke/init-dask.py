from pathlib import Path
import calrissian
import runpy
import traceback
try:
    runpy.run_path(str(Path(calrissian.__file__).parent / "dask" / "init-dask.py"), run_name="__main__")
except BaseException:
    Path("init-error.txt").write_text(traceback.format_exc())
    raise

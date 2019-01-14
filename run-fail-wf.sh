#!/bin/bash

python -m calrissian.main \
  --tmpdir-prefix /calrissian/tmptmp/ \
  --tmp-outdir-prefix /calrissian/tmpout/ \
  --outdir /calrissian/output-data \
  /calrissian/input-data/fail-wf.cwl \
  /calrissian/input-data/fail-wf-job.json



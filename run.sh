#!/bin/bash

python -m calrissian.main \
  --tmpdir-prefix /calrissian/tmptmp/ \
  --tmp-outdir-prefix /calrissian/tmpout/ \
  --outdir /calrissian/output-data \
  /calrissian/input-data/revsort-array.cwl \
  /calrissian/input-data/revsort-array-job.json



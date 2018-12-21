#!/bin/bash

cwltool --outdir=./out --tmpdir-prefix=tmp/tmp --tmp-outdir-prefix=tmp/tmpout revsort-array.cwl revsort-array-job.json

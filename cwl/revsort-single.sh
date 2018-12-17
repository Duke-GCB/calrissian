#!/bin/bash

cwltool --outdir=./out --tmpdir-prefix=tmp/tmp --tmp-outdir-prefix=tmp/tmpout revsort-single.cwl revsort-single-job.json 

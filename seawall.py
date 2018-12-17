#!/usr/bin/env python

from cwltool.main import main

argslist = [
  '--outdir=cwl/out',
  '--tmp-outdir-prefix=cwl/tmp/tmpout',
  '--tmpdir-prefix=cwl/tmp/tmp',
  'cwl/revsort-single.cwl',
  'cwl/revsort-single-job.json'
]

main(argslist)

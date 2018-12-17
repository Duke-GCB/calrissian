#!/usr/bin/env python

from cwltool.main import main
from cwltool.executors import SingleJobExecutor # Can also do multi

class SeawallExecutor(SingleJobExecutor):

  # This gets called once for the whole workflow
  def execute(self, process, job_order_object, runtime_context, logger):
    print('execute {} {} {}'.format(process, job_order_object, runtime_context))
    return super(SeawallExecutor, self).execute(process, job_order_object, runtime_context, logger)

# execute
## process=<cwltool.workflow.Workflow object at 0x10ceb1510>
## job_order_object=ordereddict([(u'input', ordereddict([(u'class', u'File'), (u'basename', u'hello.txt'), (u'contents', u'Hello World'), ('size', 11), ('location', u'_:ffb96196-3a5a-4bbc-a08b-febab4abd9c7'), ('nameroot', u'hello'), ('nameext', u'.txt')])), (u'reverse_sort', True)])
## runtime_context=<cwltool.context.RuntimeContext object at 0x10e52c9d0>
#

argslist = [
  '--outdir=cwl/out',
  '--tmp-outdir-prefix=cwl/tmp/tmpout',
  '--tmpdir-prefix=cwl/tmp/tmp',
  'cwl/revsort-single.cwl',
  'cwl/revsort-single-job.json'
]

main(argslist, executor=SeawallExecutor())

from executor import SeawallExecutor
from context import SeawallLoadingContext, SeawallRuntimeContext
from version import version
from cwltool.main import main as cwlmain
import logging

loggers = ['executor','context','tool','job', 'k8s']
for logger in loggers:
    logging.getLogger('seawall.{}'.format(logger)).setLevel(logging.DEBUG)
    logging.getLogger('seawall.{}'.format(logger)).addHandler(logging.StreamHandler())

def main():

    argslist = [
        'cwl/revsort-single.cwl',
        'cwl/revsort-single-job.json'
    ]
    logging.getLogger('seawall.context').setLevel(logging.INFO)

    executor = SeawallExecutor()
    loading_context = SeawallLoadingContext()
    # Note using the args list format, it appears I need to put the prefix args into the runtime context
    # maybe the arg parser works around this
    runtime_context = SeawallRuntimeContext({'tmpdir_prefix': 'cwl/tmp/tmpout', 'tmp_outdir_prefix': 'cwl/tmp/tmpout'})

    result = cwlmain(argslist,
                     executor=executor,
                     loadingContext=loading_context,
                     runtimeContext=runtime_context,
                     versionfunc=version,
                 )

    print(result)


if __name__ == '__main__':
    main()

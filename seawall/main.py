from executor import SeawallExecutor
from context import SeawallLoadingContext
from version import version
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
import logging

loggers = ['executor','context','tool','job', 'k8s']
for logger in loggers:
    logging.getLogger('seawall.{}'.format(logger)).setLevel(logging.DEBUG)
    logging.getLogger('seawall.{}'.format(logger)).addHandler(logging.StreamHandler())

def main():

    args = [
        '--tmpdir-prefix', './cwl/tmp/tmp',
        '--tmp-outdir-prefix', './cwl/tmp/tmpout',
        'cwl/revsort-array.cwl',
        'cwl/revsort-array-job.json'
    ]

    parser = arg_parser()
    parsed_args = parser.parse_args(args)

    result = cwlmain(args=parsed_args,
                     executor=SeawallExecutor(),
                     loadingContext=SeawallLoadingContext(),
                     versionfunc=version,
                     )
    print(result)


if __name__ == '__main__':
    main()

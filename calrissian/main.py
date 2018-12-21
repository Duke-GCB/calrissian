from executor import CalrissianExecutor
from context import CalrissianLoadingContext
from version import version
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
import logging

loggers = ['executor','context','tool','job', 'k8s']
for logger in loggers:
    logging.getLogger('calrissian.{}'.format(logger)).setLevel(logging.DEBUG)
    logging.getLogger('calrissian.{}'.format(logger)).addHandler(logging.StreamHandler())


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
                     executor=CalrissianExecutor(),
                     loadingContext=CalrissianLoadingContext(),
                     versionfunc=version,
                     )
    print(result)


if __name__ == '__main__':
    main()

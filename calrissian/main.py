from calrissian.executor import CalrissianExecutor
from calrissian.context import CalrissianLoadingContext
from calrissian.version import version
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
import logging
import sys

def activate_logging():
    loggers = ['executor','context','tool','job', 'k8s']
    for logger in loggers:
        logging.getLogger('calrissian.{}'.format(logger)).setLevel(logging.DEBUG)
        logging.getLogger('calrissian.{}'.format(logger)).addHandler(logging.StreamHandler())


def main():
    parser = arg_parser()
    parsed_args = parser.parse_args()
    result = cwlmain(args=parsed_args,
                     executor=CalrissianExecutor(),
                     loadingContext=CalrissianLoadingContext(),
                     versionfunc=version,
                     )
    return result


if __name__ == '__main__':
    activate_logging()
    sys.exit(main())

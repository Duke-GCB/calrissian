from calrissian.executor import CalrissianExecutor
from calrissian.context import CalrissianLoadingContext
from calrissian.version import version
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
from cwltool.context import RuntimeContext
import logging
import sys

def activate_logging():
    loggers = ['executor','context','tool','job', 'k8s']
    for logger in loggers:
        logging.getLogger('calrissian.{}'.format(logger)).setLevel(logging.DEBUG)
        logging.getLogger('calrissian.{}'.format(logger)).addHandler(logging.StreamHandler())


def add_arguments(parser):
    parser.add_argument('--max-ram', type=int, help='Maximum amount of RAM in MB to use')
    parser.add_argument('--max-cores', type=int, help='Maximum number of CPU cores to use')


def parse_arguments(parser):
    args = parser.parse_args()
    if not (args.max_ram and args.max_cores):
        parser.print_help()
        sys.exit(1)
    return args


def main():
    parser = arg_parser()
    add_arguments(parser)
    parsed_args = parse_arguments(parser)
    executor = CalrissianExecutor(parsed_args.max_ram, parsed_args.max_cores)
    runtimeContext = RuntimeContext(vars(parsed_args))
    runtimeContext.select_resources = executor.select_resources
    result = cwlmain(args=parsed_args,
                     executor=executor,
                     loadingContext=CalrissianLoadingContext(),
                     runtimeContext=runtimeContext,
                     versionfunc=version,
                     )
    return result


if __name__ == '__main__':
    activate_logging()
    sys.exit(main())

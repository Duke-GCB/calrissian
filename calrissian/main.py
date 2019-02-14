from calrissian.executor import CalrissianExecutor
from calrissian.context import CalrissianLoadingContext, CalrissianRuntimeContext
from calrissian.version import version
from calrissian.k8s import delete_pods
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
from typing_extensions import Text
import logging
import sys
import signal


def activate_logging():
    loggers = ['executor','context','tool','job', 'k8s']
    for logger in loggers:
        logging.getLogger('calrissian.{}'.format(logger)).setLevel(logging.DEBUG)
        logging.getLogger('calrissian.{}'.format(logger)).addHandler(logging.StreamHandler())


def add_arguments(parser):
    parser.add_argument('--max-ram', type=int, help='Maximum amount of RAM in MB to use')
    parser.add_argument('--max-cores', type=int, help='Maximum number of CPU cores to use')
    parser.add_argument('--pod-labels', type=Text, nargs='?', help='YAML file of labels to add to Pods submitted')


def print_version():
    print(version())


def parse_arguments(parser):
    args = parser.parse_args()
    # Check for version arg
    if args.version:
        print_version()
        sys.exit(0)
    if not (args.max_ram and args.max_cores):
        parser.print_help()
        sys.exit(1)
    return args


def handle_sigterm(signum, frame):
    print('Received signal {}, deleting pods'.format(signum))
    delete_pods()
    sys.exit(signum)


def install_signal_handler():
    """
    Installs a handler to cleanup submitted pods on termination.
    This is installed on the main thread and calls there on termination.
    The CalrissianExecutor is multi-threaded and will submit jobs from other threads
    """
    signal.signal(signal.SIGTERM, handle_sigterm)


def main():
    activate_logging()
    parser = arg_parser()
    add_arguments(parser)
    parsed_args = parse_arguments(parser)
    executor = CalrissianExecutor(parsed_args.max_ram, parsed_args.max_cores)
    runtime_context = CalrissianRuntimeContext(vars(parsed_args))
    runtime_context.select_resources = executor.select_resources
    install_signal_handler()
    try:
        result = cwlmain(args=parsed_args,
                         executor=executor,
                         loadingContext=CalrissianLoadingContext(),
                         runtimeContext=runtime_context,
                         versionfunc=version,
                         )
    finally:
        # Always clean up after cwlmain
        delete_pods()

    return result


if __name__ == '__main__':
    sys.exit(main())

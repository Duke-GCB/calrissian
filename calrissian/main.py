import contextlib
from calrissian.executor import ThreadPoolJobExecutor
from calrissian.context import CalrissianLoadingContext, CalrissianRuntimeContext
from calrissian.version import version
from calrissian.k8s import delete_pods
from calrissian.report import initialize_reporter, write_report, CPUParser, MemoryParser
from cwltool.main import main as cwlmain
from cwltool.argparser import arg_parser
from typing_extensions import Text
import logging
import sys
import signal
import subprocess
import os
import shlex
import json

log = logging.getLogger("calrissian.main")


def get_log_level(parsed_args):
    level = logging.WARNING
    if parsed_args.quiet:
        level = logging.CRITICAL
    elif parsed_args.verbose:
        level = logging.INFO
    elif parsed_args.debug:
        level = logging.DEBUG
    return level


def activate_logging(level):
    loggers = ['executor','context','tool','job', 'k8s','main']
    for logger in loggers:
        logging.getLogger('calrissian.{}'.format(logger)).setLevel(level)
        logging.getLogger('calrissian.{}'.format(logger)).addHandler(logging.StreamHandler())


def add_arguments(parser):
    parser.add_argument('--max-ram', type=str, help='Maximum amount of RAM to use, e.g 1048576, 512Mi or 2G. Follows k8s resource conventions')
    parser.add_argument('--max-cores', type=str, help='Maximum number of CPU cores to use')
    parser.add_argument('--max-gpus', type=str, nargs='?', help='Maximum number of GPU cores to use')
    parser.add_argument('--pod-labels', type=Text, nargs='?', help='YAML file of labels to add to Pods submitted')
    parser.add_argument('--pod-env-vars', type=Text, nargs='?', help='YAML file of environment variables to add at runtime to Pods submitted')
    parser.add_argument('--pod-nodeselectors', type=Text, nargs='?', help='YAML file of node selectors to add to Pods submitted')
    parser.add_argument('--pod-serviceaccount', type=str, help='Service Account to use for pods management')
    parser.add_argument('--usage-report', type=Text, nargs='?', help='Output JSON file name to record resource usage')
    parser.add_argument('--stdout', type=Text, nargs='?', help='Output file name to tee standard output (CWL output object)')
    parser.add_argument('--stderr', type=Text, nargs='?', help='Output file name to tee standard error to (includes tool logs)')
    parser.add_argument('--tool-logs-basepath', type=Text, nargs='?', help='Base path for saving the tool logs')
    parser.add_argument('--conf', help='Defines the default values for the CLI arguments', action='append')


def print_version():
    print(version())


def parse_arguments(parser):
    
    # read default config from file
    args = parser.parse_args()

    with contextlib.suppress(KeyError, FileNotFoundError):
        with open(os.path.join(os.environ["HOME"], ".calrissian", "default.json"), 'r') as f:
            parser.set_defaults(**json.load(f))
    
    if args.conf is not None:
        
        with open(args.conf[0], 'r') as f:
            parser.set_defaults(**json.load(f))

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
    log.error('Received signal {}, deleting pods'.format(signum))
    delete_pods()
    sys.exit(signum)


def install_signal_handler():
    """
    Installs a handler to cleanup submitted pods on termination.
    This is installed on the main thread and calls there on termination.
    The CalrissianExecutor is multi-threaded and will submit jobs from other threads
    """
    signal.signal(signal.SIGTERM, handle_sigterm)


def install_tees(stdout_path=None, stderr_path=None):
    """
    Reconnects stdout/stderr to `tee` processes via subprocess.PIPE that can write to user-supplied files
    https://stackoverflow.com/a/651718/595085
    :param stdout_path: optional path of file to tee standard output to
    :param stderr_path: optional path of file to tee standard error to
    :return: None
    """

    if stdout_path:
        stdout_tee_process = subprocess.Popen(["tee", stdout_path], stdin=subprocess.PIPE)
        os.dup2(stdout_tee_process.stdin.fileno(), sys.stdout.fileno())

    if stderr_path:
        # stderr must be handled differently. By default, tee sends output to stdout,
        # so we run it under a shell to redirect that to stderr, and use shlex.quote for safety
        stderr_tee_process = subprocess.Popen(["tee >&2 {}".format(shlex.quote(stderr_path))],
                                              stdin=subprocess.PIPE,
                                              shell=True)
        os.dup2(stderr_tee_process.stdin.fileno(), sys.stderr.fileno())


def flush_tees():
    sys.stdout.flush()
    sys.stderr.flush()


def main():
    parser = arg_parser()
    add_arguments(parser)
    parsed_args = parse_arguments(parser)
    level = get_log_level(parsed_args)
    activate_logging(level)
    install_tees(parsed_args.stdout, parsed_args.stderr)
    max_ram_megabytes = MemoryParser.parse_to_megabytes(parsed_args.max_ram)
    max_cores = CPUParser.parse(parsed_args.max_cores)
    max_gpus = int(parsed_args.max_gpus) if parsed_args.max_gpus else 0
    executor = ThreadPoolJobExecutor(max_ram_megabytes, max_cores, max_gpus)
    initialize_reporter(max_ram_megabytes, max_cores)
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
        if parsed_args.usage_report:
            write_report(parsed_args.usage_report)
        flush_tees()

    return result


if __name__ == '__main__':
    sys.exit(main())

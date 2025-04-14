import pathvalidate
import sys
import os
from argparse import ArgumentParser
from file_synchronizer import FileSynchronizer


def validate_args(args):
    """
    Function's purpose is to validate whether according to system rules, paths provided as command
    line arguments ar valid, i.e. if it is possible to create such paths in the system (because of
    system restrictions)
    :param args: argparse.Namespace containing all command line arguments program was called with:
    :return: None
    """
    if not pathvalidate.is_valid_filepath(args.source, platform="auto"):
        print(f"    ERROR: Path to source directory is invalid: '{args.source}'")
        return False
    if not os.path.exists(args.source):
        print(f"    ERROR: Source directory does not exist: '{args.source}'")
        return False
    if not pathvalidate.is_valid_filepath(args.replica, platform="auto"):
        print(f"    ERROR: Path to replica directory is invalid: '{args.replica}'")
        return False
    if not pathvalidate.is_valid_filepath(args.logfile, platform="auto"):
        print(f"    ERROR: Path to logfile is invalid: '{args.logfile}'")
        return False
    return True

def parse_args():
    """
    Function's purpose is to parse command line arguments and store them in an appropriate object
    :return:
    """
    parser = ArgumentParser(
        prog="folder-synchronizer",
        usage='%(prog)s [options]')
    parser.add_argument(
        "--source", "-s", type=str, required=True,
        help="Path to folder to be used as source for synchronization")
    parser.add_argument(
        "--replica", "-r", type=str, required=True,
        help="Path to folder that should be synchronized with Source folder")
    parser.add_argument(
        "--logfile", "-l", type=str, required=True,
        help="Location where to store logfile from application execution")
    parser.add_argument(
        "--period", "-p", type =int, default=300,
        help="Synchronization time period, default 300 seconds")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if not validate_args(args):
        sys.exit()
    fs = FileSynchronizer(args.source, args.replica, args.logfile, args.period)
    fs.initialize()
    fs.run()
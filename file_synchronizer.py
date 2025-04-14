import datetime
import logging as logging
import os.path
import threading
import hashlib
import time
import shutil
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler


class FileSynchronizer:
    """
    FileSynchronizer is a class, which contains all functions performing synchronization between source and replica
    directories,
    """
    def __init__(self, source: str, replica: str, logfile: str, period: int):
        self.logger = None
        self.source_dir = source
        self.replica_dir = replica
        self.period = period
        self.logfile = logfile

    def initialize(self) -> None:
        """
        Function creates logger for logging purposes and replica folder in case the one provided does not exist
        :return: None
        """
        self.logger = self.create_logger(self.logfile)
        if not os.path.exists(self.replica_dir):
            os.makedirs(self.replica_dir)
            self.logger.debug(f"Replica directory: [{self.replica_dir}] created.")

    @staticmethod
    def create_logger(logfile: str) -> logging.Logger:
        """
        Function creates logger for logging purposes with two different handlers, one logging into file, second
        logging into console output.
        :param logfile: A str link for a file where file handler should log events into.
        :return: logging.Logger object type, ready to be used to log events.
        """

        # logger creation, logging level setting and logging format definition
        logger = logging.getLogger("folder_synchronizer")
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s] - [%(levelname)s] - %(message)s',
            "%Y-%m-%d %H:%M:%S")

        # file handler creation and its registration to logger
        file_handler = TimedRotatingFileHandler(logfile, when="W0")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # stream handler (console) creation and its registration to logger
        console_handler = StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def run(self) -> None:
        """
        Main function for cyclic execution of synchronization between source and replica directories.
        It executes synchronization when last synchronization is complete and was done a defined time (as period) ago.
        :return: None
        -----------------------------------------------------------------------
        Comment:
            Several different approaches could have also been used:
            - threading.Time (with its interval parameter)
            - timeloop library (which generally bases on threading.Timer)
            - FileSynchronization class could have inherited from threading.Thread and be adjusted for use
            ...
        """
        prev_call_time = datetime.datetime.now()
        t = threading.Thread(target=self.synchronize_directories)
        t.start()
        while True:
            time.sleep(1)
            curr_call_time = datetime.datetime.now()
            if curr_call_time - prev_call_time >= timedelta(seconds=self.period) and not t.is_alive():
                prev_call_time = curr_call_time
                t = threading.Thread(target=self.synchronize_directories)
                t.start()

    def synchronize_directories(self) -> None:
        """
        Function performing synchronization between source and replica in two-step approach:
            Step1: gathers all items in source directory and performs check if any of these items
                are missing in replica directory or if (in case of files) they do exist but differ
            Step2: gathers all items in replica directory and performs check if any of these items
                are not found in source directory - deletes them in replica as redundancy
        :return:
        """
        # Step1
        items_to_sync = self.fetch_files_and_dirs(self.source_dir)
        self.update_replica(items_to_sync)

        #Step2:
        items_to_verify = self.fetch_files_and_dirs(self.replica_dir)
        self.cleanup_replica(items_to_verify)

    def update_replica(self, items_to_sync) -> None:
        """
        Function performing synchronization of items from source directory to replica directory, creating missing
        directories and copying missing files.
        :param items_to_sync: complete list of items from source directory, containing absolute paths
        :return: None
        """
        # For better performance check is done as one pass through all items in the list
        for source_path in items_to_sync:
            relative_path = os.path.relpath(source_path, self.source_dir)
            replica_path = os.path.join(self.replica_dir, relative_path)

            # Check if the item to sync is a directory and if it does not exist in replica, create it
            if os.path.isdir(source_path):
                if not os.path.exists(replica_path):
                    try:
                        os.makedirs(replica_path)
                        self.logger.debug(f"Directory [{replica_path}] created.")
                    except PermissionError as e:
                        self.logger.warning(f"Could not create directory: [{replica_path}]. Permission error: {e}")

            # If item to sync is file and if it does not exist in replica or differs from the one in source, copy it
            # Although generation and comparison of md5 has been implemented, filecmp library could have also been used
            else:
                if not os.path.exists(replica_path) or not self.files_equal(source_path, replica_path):
                    try:
                        shutil.copy2(source_path, replica_path)
                        self.logger.debug(f"File [{replica_path}] copied from [{source_path}]")
                    except PermissionError as e:
                        self.logger.warning(f"Could not copy file: [{replica_path}]. Permission error: {e}")

    def cleanup_replica(self, items_to_verify) -> None:
        """
        Function performing synchronization of items in replica directory in reference to source directory, removing
        items that are redundant (not found in source directory).
        :param items_to_verify: complete list of items from replica directory, containing absolute paths
        :return: None
        """
        for replica_path in items_to_verify:
            relative_path = os.path.relpath(replica_path, self.replica_dir)
            source_path = os.path.join(self.source_dir, relative_path)

            # Check if item does not exist in source directory and still exists in replica directory because whole
            # directory removal might remove all items under specific directory, thus further removal of its items
            # is not possible
            if not os.path.exists(source_path) and os.path.exists(replica_path):

                # in case of folder redundancy shutil library is used
                if os.path.isdir(replica_path):
                    try:
                        shutil.rmtree(replica_path)
                        self.logger.debug(f"Directory [{replica_path}] removed.")
                    except PermissionError as e:
                        self.logger.warning(f"Could not remove directory: [{replica_path}]. Permission error: {e}.")

                # in case of file redundancy simply os library is used
                else:
                    try:
                        os.remove(replica_path)
                        self.logger.debug(f"File [{replica_path}] removed.")
                    except PermissionError as e:
                        self.logger.warning(f"Could not remove file: [{replica_path}]. Permission error: {e}.")

    def files_equal(self, l, r) -> bool:
        """
        Function checks if files are equal using generated md5 hash
        :param l: left file object
        :param r: right file object
        :return: bool value pointing if both files equal or not
        """
        if self.generate_md5(l) == self.generate_md5(r):
            return True
        return False

    @staticmethod
    def fetch_files_and_dirs(directory) -> list:
        """
        Function gathering all items under a directory using os.walk function.
        :param directory: Contains absolute path to a directory to gather its children elements
        :return: list of all children of a directory.
        """
        items_to_sync = []
        for root, dirs, files in os.walk(directory):

            # concatenates root directory with all subdirectories walked through
            for d in dirs:
                items_to_sync.append(os.path.join(root, d))

            # concatenates root directory with all files walked through
            for f in files:
                items_to_sync.append(os.path.join(root, f))

        return items_to_sync

    @staticmethod
    def generate_md5(path, chunk_size=4096) -> str:
        """
        Function generates md5 hash using hashlib library for a file.
        :param path: Path to a file for which md5 is to be generated.
        :param chunk_size: Chunk by which file content is read. It ts done in chunks to prevent memory overflow in case
            of very big files
        :return: md5 hash in str format.
        """
        md5_hash = hashlib.md5()
        with open(path, "rb") as f:
            chunk = f.read(chunk_size)
            while chunk:
                md5_hash.update(chunk)
                chunk = f.read(chunk_size)
        return md5_hash.hexdigest()
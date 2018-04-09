"""File with class for job objects."""

import os


class Job:
    """Class defining jobs for execution
    """

    def __init__(self, name, executable, priority, time_required, min_memory,
                 min_cores, max_memory):
        """Initializes job object.

        :param name: str, name of the job
        :param executable: str, address of the executable file
        :param priority:  int, -20 to 20 denoting priority
        :param time_required: float, seconds of time required
        :param min_memory: int, min amount of memory (in MB) required for
            execution
        :param min_cores: int, min no. of cores required for execution
        :param max_memory: int, max amount of memory (in MB) preferred for
            execution
        """

        self.submission_id = None
        self.receipt_id = None
        self.name = name
        self.username = os.uname()[1]
        self.executable = executable
        self.priority = priority
        self.time_required = time_required
        self.time_run = 0

        # Requirements
        self.min_memory = min_memory
        self.min_cores = min_cores
        self.max_memory = max_memory

    def get_executable_name(self):
        """ Return name of the executable file, by parsing the address.
        """

        executable_address_partitions = self.executable.split('/')
        return '/' + executable_address_partitions[-1]

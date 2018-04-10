"""File with class for job objects."""

import os


class Job:
    """Class defining jobs for execution

    Attributes:
        submission_id: int, id given by the submitting node
        receipt_id: int, id given by the receiving server
        name: str, job name
        username: str, owner of job submitting machine
        executable: str, path to executable file on submitting machine
        priority:  int, -20 to 20 denoting priority, higher num implies
            higher priority
        min_memory: int, min amount of memory (in MB) required for
            execution
        min_cores: int, min no. of cores required for execution
        max_memory: int, max amount of memory (in MB) preferred for
            execution
        time_run: float, number of seconds job has executed on a CPU. Note
            that we don't use exact time for which CPU executed it.
        completed: bool, whether job has completed execution.

    """
    def __init__(self, name, executable, priority, time_required, min_memory,
                 min_cores, max_memory):
        """Initializes job object.

        :param name: str, name of the job
        :param executable: str, path to executable file on submitting machine
        :param priority:  int, -20 to 20 denoting priority
        :param time_required: float, seconds of time required for completion
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

        # Requirements
        self.min_memory = min_memory
        self.min_cores = min_cores
        self.max_memory = max_memory

        # Job state
        self.time_run = 0
        self.completed = False

    def get_executable_name(self):
        """ Return name of the executable file, by parsing the address."""
        executable_address_partitions = self.executable.split('/')
        return '/' + executable_address_partitions[-1]

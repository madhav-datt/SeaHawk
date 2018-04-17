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
        execution_list: list, containing tuple elements
            (executing machine name, time_run) for each machine that executes
            this job
        submit_time: time at which job is submitted to central server
        submission_completion_time: time at which submitting node receives
            completed job back
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
        self.sender = None
        self.name = name
        self.username = os.uname()[1]
        self.executable = executable
        self.priority = int(priority)
        self.time_required = int(time_required)

        # Requirements
        self.min_memory = int(min_memory)
        self.min_cores = int(min_cores)
        self.max_memory = int(max_memory)

        # Job state
        self.time_run = 0
        self.completed = False
        self.execution_list = []
        self.submit_time = None
        self.first_response = None
        self.receive_time = None
        self.submission_completion_time = None

    def get_executable_name(self):
        """ Return name of the executable file, by parsing the address.
        """
        executable_address_partitions = self.executable.split('/')
        return '/' + executable_address_partitions[-1]

    def __str__(self):
        """Custom function to print job details.

        :return: String representation of job object.
        """
        return 'JOB: ' + str(self.submission_id) + ',' + str(self.receipt_id)

    def __eq__(self, other):
        """Job equality comparison based on unique ID assigned by server.

        :param other: Job object
        :return: Boolean with job equality value
        """
        return (self.receipt_id == other.receipt_id) or (
                self.submission_id == other.submission_id and
                self.sender == other.sender and
                self.sender is not None)

    def __lt__(self, other):
        """Job ordering comparison based on job priority.

        :param other: Job object
        :return: Boolean with job equality value
        """
        return self.priority < other.priority

    def __hash__(self):
        """Makes job objects hashable based on sender assigned details.

        :return: hash values based on tuple (submission_id, sender)
        """
        return hash((self.submission_id, self.sender))

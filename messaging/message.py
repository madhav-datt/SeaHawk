"""File with class for message objects.
"""


class Message(object):
    """Class defining messages to be passed for system communication.

    Attributes:
          msg_type: String with type of message.
          content: Data structure/string with message contents.
                Default: None.
                HEARTBEAT: Dictionary of system resources available.
                JOB_SUBMIT: Job object for the submitted job.
                JOB_EXEC: Job object for the submitted job.
                JOB_EXEC_PREEMPT: Tuple with (Job object for the submitted job,
                    Receipt id of job to be preempted)
                EXECUTED_JOB: Job object for the executing/preempted job
                ACK_JOB_SUBMIT: Job submission id, int.
                ACK_EXECUTED_JOB: Receipt id of job
                SUBMITTED_JOB_COMPLETION: Job object for the submitted job.
                ACK_SUBMITTED_JOB_COMPLETION: Receipt id of submitted job.
                NODE_CRASH: Set of nodes that have crashed.
                SERVER_CRASH: None
                BACKUP_UPDATE: ServerState object with latest state of server.
          sender: String with IP address of sender machine.
          file: Byte string with file contents.
                Default: None
                HEARTBEAT: None
                JOB_SUBMIT: executable
                JOB_EXEC: executable
                JOB_EXEC_PREEMPT: executable
                EXECUTED_JOB: None
                ACK_JOB_SUBMIT: None
                ACK_EXECUTED_JOB: None
                SUBMITTED_JOB_COMPLETION: Generated log file
                ACK_SUBMITTED_JOB_COMPLETION: None
                NODE_CRASH: None
                SERVER_CRASH: None
                BACKUP_UPDATE: None
    """

    def __init__(self, msg_type, content=None, file_path=None, file=None):
        """Initializes Message object with type parameters and adds content.

        Args:
            msg_type: String with type of message.
            content: Data structure/string with message contents.
            file_path: String with absolute path to file to be included. If file
                is also given, it will be ignored.
            file: Byte stream of file. If file_path is not None, this will be
                ignored.
        """
        self.msg_type = msg_type
        self.content = content

        self.sender = None
        self.file = file
        if file_path is not None:
            with open(file_path, 'rb') as file:
                self.file = file.read()

    def __str__(self):
        """Custom function to print message details.

        :return: String representation of message information.
        """
        return self.msg_type + ' ' + str(self.content)

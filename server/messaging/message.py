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
                JOB_PREEMPT: Receipt id of job
                EXECUTED_JOB: Job object for the executing/preempted job
                ACK_JOB_SUBMIT: Empty?
                ACK_EXECUTED_JOB: Receipt id of job
                SUBMITTED_JOB_COMPLETION: Job object for the submitted job.
                ACK_SUBMITTED_JOB_COMPLETION: Receipt id of submitted job
          sender: String with IP address of sender machine.
          file: Byte string with file contents.
                Default: None
                HEARTBEAT: None
                JOB_SUBMIT: executable
                JOB_EXEC: executable
                JOB_PREEMPT: None
                EXECUTED_JOB: None
                ACK_JOB_SUBMIT: None
                ACK_EXECUTED_JOB: None
                SUBMITTED_JOB_COMPLETION: Generated log file
                ACK_SUBMITTED_JOB_COMPLETION: None
    """

    def __init__(self, msg_type, content=None, file_path=None):
        """Initializes Message object with type parameters and adds content.

        Args:
            msg_type: String with type of message.
            content: Data structure/string with message contents.
            file_path: String with absolute path to file to be included.
        """

        self.msg_type = msg_type
        self.content = content

        self.sender = None
        self.file = None
        if file_path is not None:
            with open(file_path, 'rb') as file:
                self.file = file.read()

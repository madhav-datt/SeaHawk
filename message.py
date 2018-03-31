"""File with class for message objects.
"""


class Message(object):
    """Class defining messages to be passed for system communication.

    Attributes:
          msg_type: String with type of message.
          content: String with message contents. None by default.
          sender: String with IP address of sender machine.
          file: Byte string with file contents. None by default.
    """

    def __init__(self, msg_type, content=None, file_path=None):
        """Initializes Message object with type parameters and adds content.

        Args:
            msg_type: String with type of message.
            content: String with message contents.
            file_path: String with absolute path to file to be included.
        """

        self.msg_type = msg_type
        self.content = content

        self.sender = None
        self.file = None
        if file_path is not None:
            with open(file_path, 'rb') as file:
                self.file = file.read()

    @property
    def msg_type(self):
        return self.msg_type

    @property
    def content(self):
        return self.content

    @property
    def file(self):
        return self.file

    @property
    def sender(self):
        return self.sender

    @msg_type.setter
    def msg_type(self, msg_type):
        self.msg_type = msg_type

    @content.setter
    def content(self, content):
        self.content = content

    @file.setter
    def file(self, file):
        self.file = file

    @sender.setter
    def sender(self, sender):
        self.sender = sender

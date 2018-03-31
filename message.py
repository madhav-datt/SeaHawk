"""File with class for message objects.
"""


class Message(object):
    """Class defining messages to be passed for system communication.

    Attributes:
          type: String with type of message.
          content: String with message contents. None by default.
          file: Byte string with file contents. None by default.
    """

    def __init__(self, type, content=None, file=None):
        """Inits Message object with type parameters and adds content.

        Args:
            type: String with type of message.
            content: String with message contents.
            file: String with absolute path to file to be included.
        """

        self.type = type
        self.content = content
        self.file = file.read()

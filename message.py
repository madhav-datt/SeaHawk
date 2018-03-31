"""File with class for message objects.
"""


class Message(object):
    """Class defining messages to be passed for system communication.

    Attributes:
          type: String with type of message.
          content: String with message contents. None by default.
          file: Byte string with file contents. None by default.
    """

    def __init__(self, type, content=None, file_path=None):
        """Inits Message object with type parameters and adds content.

        Args:
            type: String with type of message.
            content: String with message contents.
            file_path: String with absolute path to file to be included.
        """

        self.type = type
        self.content = content

        with open(file_path, 'rb') as file:
            self.file = file.read()

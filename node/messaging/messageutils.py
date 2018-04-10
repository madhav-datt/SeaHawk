"""File with helper/util functions to support message passing.
"""

import io
import pickle
import psutil
import socket

from . import message

BUFFER_SIZE = 1048576
PORT = 5005


def send_message(msg, to, msg_socket=None, port=PORT):
    """Sends binary/pickle of message object to receiver.

    :param msg: Message object with data of message to be sent
    :param to: String with IP address of receiver node
    :param msg_socket: Socket object on which message is to be sent. Opens new
        socket if value is None.
    :param port: Integer with port to be used for sending/receiving messages.
        Default is 5005.
    """

    if msg_socket is None:
        msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            msg_socket.connect((to, port))
        except OSError:
            # Raised if endpoint is already connected. No action is needed.
            pass
    msg.sender = msg_socket.getsockname()[0]
    msg_data = io.BytesIO(pickle.dumps(msg))

    while True:
        chunk = msg_data.read(BUFFER_SIZE)
        if not chunk:
            break
        msg_socket.send(chunk)

    msg_socket.shutdown(socket.SHUT_WR)
    msg_socket.close()


def send_heartbeat(to, msg_socket=None, port=PORT):
    """Sends heartbeat message with system resource usage information.

    Heartbeat message includes current CPU usage percentage and memory available
    for use by new jobs issued at the system.

    :param to: String with IP address of receiver node
    :param msg_socket: Socket object on which message is to be sent. Opens new
        socket if value is None.
    :param port: Integer with port to be used for sending/receiving messages.
        Default is 5005.
    """

    # 'cpu': Percent CPU available, 'memory': Available memory in MB
    system_resources = {
        'cpu': 100 - psutil.cpu_percent(),
        'memory': psutil.virtual_memory().available >> 20,
    }
    heartbeat_msg = message.Message(
        msg_type='HEARTBEAT', content=system_resources)
    send_message(msg=heartbeat_msg, to=to, port=port, msg_socket=msg_socket)

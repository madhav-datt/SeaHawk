"""File with helper/util functions to support message passing.
"""

import io
import pickle
import psutil
import socket
import time

from . import message
from .network_params import BUFFER_SIZE

HEARTBEAT_REPLY_WAIT_SECONDS = 5
PORT = 5005


def make_and_send_message(msg_type, content, file_path, to, msg_socket, port):
    """Construct a message object with given attributes & send to address

    :param msg_type: str, one of the pre-defined message types
    :param content: obj, custom message content
    :param file_path: str, destination address of file to accompany message
    :param to: str, ip address of destination machine
    :param msg_socket: socket, via which to send the message
    :param port: int, port number on which message should be received
    """
    msg = message.Message(
        msg_type=msg_type, content=content, file_path=file_path)
    send_message(msg=msg, to=to, msg_socket=msg_socket, port=port)


def wait_send_heartbeat_to_backup(to, port, server_state):
    """Wait for a short duration and send heartbeat message to backup.

    Function must be run as a forked child process.

    :param to: String with IP address of receiver node
    :param port: Integer with port to be used for sending/receiving messages.
    :param server_state: ServerState object with state to be sent to backup.
    """
    time.sleep(HEARTBEAT_REPLY_WAIT_SECONDS)
    make_and_send_message(
        msg_type='HEARTBEAT',
        content=server_state,
        file_path=None,
        to=to,
        msg_socket=None,
        port=port)


def wait_send_heartbeat(to, port):
    """Wait for a short duration and send heartbeat message.

    Function must be run as a forked child process.

    :param to: String with IP address of receiver node
    :param port: Integer with port to be used for sending/receiving messages.
    """
    time.sleep(HEARTBEAT_REPLY_WAIT_SECONDS)
    send_heartbeat(to=to, port=port)


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

    try:
        while True:
            chunk = msg_data.read(BUFFER_SIZE)
            if not chunk:
                break
            msg_socket.send(chunk)
    except BrokenPipeError:
        # Connection with end-point broken due to node crash.
        # Do nothing as crash will be handled by crash detector and handler.
        pass

    try:
        msg_socket.shutdown(socket.SHUT_WR)
        msg_socket.close()
    except OSError:
        # Connection with end-point broken due to node crash.
        # Do nothing as crash will be handled by crash detector and handler.
        pass


def send_heartbeat(to, msg_socket=None, port=PORT, num_executing_jobs=None):
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
    memory = psutil.virtual_memory().available >> 20
    if num_executing_jobs is not None:
        memory = max(300, memory - num_executing_jobs * 300)

    system_resources = {
        'cpu': 100 - psutil.cpu_percent(),
        'memory': memory,
    }

    # Construct the message with system resources and send to server
    make_and_send_message(
        msg_type='HEARTBEAT',
        content=system_resources,
        file_path=None,
        to=to,
        msg_socket=msg_socket,
        port=port)

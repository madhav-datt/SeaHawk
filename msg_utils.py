"""File with helper/util functions to support message passing.
"""

import io
import message
import pickle
import psutil
import socket

BUFFER_SIZE = 1024
PORT = 5005


def send_message(msg, to, port=PORT):
    """Sends binary/pickle of message object to receiver.

    :param msg: Message object with data of message to be sent
    :param to: String with IP address of receiver node
    :param port: Integer with port to be used for sending/receiving messages.
        Default is 5005.
    """

    msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    msg_socket.connect((to, port))
    msg.sender = msg_socket.getsockname()[0]
    msg_data = io.BytesIO(pickle.dumps(msg))

    while True:
        chunk = msg_data.read(BUFFER_SIZE)
        if not chunk:
            break
        msg_socket.send(chunk)

    msg_socket.shutdown(socket.SHUT_WR)
    msg_socket.close()


def send_heartbeat(to, port=PORT):
    """Sends heartbeat message with system resource usage information.

    Heartbeat message includes current CPU usage percentage and memory available
    for use by new jobs issued at the system.

    :param to: String with IP address of receiver node
    :param port: Integer with port to be used for sending/receiving messages.
        Default is 5005.
    """

    # 'cpu': Percent CPU available, 'memory': Available memory in MB
    system_resources = {
        'cpu': 100 - psutil.cpu_percent(),
        'memory': psutil.virtual_memory().available >> 20
    }
    heartbeat_msg = message.Message(
        msg_type='HEARTBEAT', content=system_resources)
    send_message(heartbeat_msg, to, port)
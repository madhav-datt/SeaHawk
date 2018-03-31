"""File with library to send messages.
"""

import message
import pickle
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
    msg_data = pickle.dump(message)

    l = f.read(1024)
    while l:
        msg_socket.send(l)
        l = f.read(1024)
    f.close()

    msg_socket.shutdown(socket.SHUT_WR)
    print msg_socket.recv(1024)
    msg_socket.close()
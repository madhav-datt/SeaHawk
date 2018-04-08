"""Script to run computing node.
"""

import argparse
import socket
import pickle

PORT = 5005
BUFFER_SIZE = 1048576


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Set up computing node.')
    parser.add_argument(
        '--ip', required=True, help='IP address of central server.')
    args = parser.parse_args()

    msg_socket = socket.socket()
    msg_socket.bind(('', PORT))
    msg_socket.listen(1)

    while True:
        connection, client_address = msg_socket.accept()
        data = connection.recv(BUFFER_SIZE)
        msg = pickle.loads(data)

        connection.close()

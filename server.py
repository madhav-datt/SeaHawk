import argparse
import select
import socket
import sys

PORT = 5005


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument('--ip')
    args = parser.parse_args()

    # Create a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(0)

    # Bind the socket to the port
    server_address = ('localhost', PORT)
    print(sys.stderr, 'starting up on %s port %s' % server_address)
    server.bind(server_address)
    server.listen(5)

    # Sockets for reading and writing
    inputs = [server]
    outputs = []

    while inputs:

        # Wait for at least one of the sockets to be ready for processing
        print(sys.stderr, '\nwaiting for the next event')
        readable, _, _ = select.select(inputs, outputs, inputs)

        # Handle inputs
        for s in readable:

            if s is server:
                # A "readable" server socket is ready to accept a connection
                connection, client_address = s.accept()
                print(sys.stderr, 'new connection from', client_address)
                connection.setblocking(0)
                inputs.append(connection)

            else:
                data = s.recv(1024)
                if data:
                    print(data)
                    # TODO Read the whole message.
                    # TODO Covert to Message object from pickle.
                    # TODO Call handler according to message type.
                    # TODO Socket option in send_message function.
                    # TODO Add message handlers and types etc. ***

                else:
                    print(sys.stderr, 'closing', client_address,
                          'after reading no data')
                    inputs.remove(s)
                    s.close()

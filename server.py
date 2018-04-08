import argparse
import serverhandlers
import pickle
import select
import socket
import sys

PORT = 5005
BUFFER_SIZE = 1048576

compute_nodes = {}

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument(
        '--ip', required=True, help='IP address of central server (this node).')
    parser.add_argument(
        '--nodes-file',
        required=True,
        help='Absolute path to txt file with IP addresses of each '
             'client/computing node.')
    args = parser.parse_args()

    with open(args.node_file) as nodes_ip_file:
        for node_ip in nodes_ip_file:
            compute_nodes[node_ip[:-1]] = {
                'cpu': None, 'memory': None, 'last_seen': None
            }

    # Create a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(0)

    # Bind the socket to the port
    server_address = (args.ip, PORT)
    print(sys.stderr, 'starting up on %s port %s' % server_address)
    server.bind(server_address)
    server.listen(5)

    # Sockets for reading and writing
    inputs = [server]
    outputs = []
    client_address = None

    while inputs:

        # Wait for at least one of the sockets to be ready for processing
        print(sys.stderr, '\nwaiting for the next event')
        readable, _, _ = select.select(inputs, outputs, inputs)

        # Handle inputs
        for msg_socket in readable:

            if msg_socket is server:
                # A "readable" server socket is ready to accept a connection
                connection, client_address = msg_socket.accept()
                print(sys.stderr, 'new connection from', client_address)
                connection.setblocking(0)
                inputs.append(connection)

            else:
                data = msg_socket.recv(BUFFER_SIZE)
                if data:
                    msg = pickle.loads(data)

                    if msg.msg_type == 'HEARTBEAT':
                        serverhandlers.heartbeat_handler()

                else:
                    print(sys.stderr, 'closing', client_address,
                          'after reading no data')
                    inputs.remove(msg_socket)
                    msg_socket.close()

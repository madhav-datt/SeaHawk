"""Script to run central server.
"""

import argparse
import matchmaking
import messageutils
import pickle
import priorityqueue
import select
import socket
import sys
import time

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006
BUFFER_SIZE = 1048576

compute_nodes = {}
node_list = []
job_queue = priorityqueue.JobQueue()
wait_queue = []


def heartbeat_handler(received_msg):
    """Handler function for HEARTBEAT messages.

    :param received_msg: message, received message.
    """

    compute_nodes[received_msg.sender]['cpu'] = received_msg['cpu']
    compute_nodes[received_msg.sender]['memory'] = received_msg['memory']
    compute_nodes[received_msg.sender]['last_seen'] = time.time()

    # Send heartbeat message to computing node
    messageutils.send_heartbeat(to=received_msg.sender, port=SERVER_SEND_PORT)


def job_submit_handler(received_msg):
    """Handler function for JOB_SUBMIT messages.

    :param received_msg: message, received message.
    """

    pass


def main():
    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument(
        '--ip', required=True, help='IP address of central server (this node).')
    parser.add_argument(
        '--nodes-file',
        required=True,
        help='Absolute path to txt file with IP address, total memory of each '
             'client/computing node.')
    args = parser.parse_args()

    with open(args.node_file) as nodes_ip_file:
        for node_ip in nodes_ip_file:
            ip_address, total_memory = node_ip[:-1].split(',')
            matchmaking.running_jobs[ip_address] = []
            node_list.append(ip_address)
            compute_nodes[ip_address] = {
                'cpu': None, 'memory': None, 'last_seen': None,
                'total_memory': total_memory,
            }

    # Creates a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(0)

    # Binds the socket to the port
    server_address = ('', SERVER_RECV_PORT)
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
                        heartbeat_handler(msg)
                    elif msg.msg_type == 'JOB_SUBMIT':
                        job_submit_handler(msg)

                else:
                    print(sys.stderr, 'closing', client_address,
                          'after reading no data')
                    inputs.remove(msg_socket)
                    msg_socket.close()


if __name__ == '__main__':
    main()

"""Script to run central server.

    Responsible for communication with computing nodes, primary backup, job
    scheduling, load balancing, job-node matchmaking decisions etc.

    Messages received from node:

    Messages received from backup:

    Messages sent to node:

    Messages sent to backup:

"""

import argparse
import multiprocessing as mp
import pickle
import select
import socket
import sys
import time

from . import message_handlers
from .messaging import messageutils
from .utils import priorityqueue

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006
BUFFER_SIZE = 1048576
CRASH_ASSUMPTION_TIME = 20  # seconds
CRASH_DETECTOR_SLEEP_TIME = 2  # seconds

compute_nodes = {}  # {node_id: status}
node_list = []
job_queue = priorityqueue.JobQueue()
running_jobs = {}  # {node_id: [list of jobs]}
job_executable = {}  # {job_id: executable}
job_sender = {}  # {job_id: sender}


def detect_node_crash(node_last_seen):
    """Detects node crashes.

    Run as a child process, periodically checking last heartbeat times for each
    computing node.

    :param node_last_seen: Dictionary with time when last heartbeat was
        received from node {node_id: last_seen_time}
    """

    while True:
        time.sleep(CRASH_DETECTOR_SLEEP_TIME)

        current_time = time.time()
        crashed_nodes = set()
        for node_id, last_seen_time in node_last_seen.items():
            time_since_last_heartbeat = current_time - last_seen_time
            if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
                crashed_nodes.add(node_id)

        # Make and send a crash message to main process which is listening
        # on SERVER_RECV_PORT for incoming messages.
        # TODO: Check if 'to' needs to be changed to socket.gethostname()
        if len(crashed_nodes) != 0:
            messageutils.make_and_send_message(msg_type='NODE_CRASH',
                                               content=crashed_nodes,
                                               file_path=None,
                                               to='127.0.0.1',
                                               msg_socket=None,
                                               port=SERVER_RECV_PORT)


def main():
    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument(
        '--server-ip',
        required=True,
        help='IP address of central server (this node).')
    parser.add_argument(
        '--backup-ip',
        required=True,
        help='IP address of primary backup server.')
    parser.add_argument(
        '--nodes-file',
        required=True,
        help='Absolute path to txt file with IP address, total memory of each '
             'client/computing node.')
    args = parser.parse_args()
    backup_ip = args.backup_ip

    job_receipt_id = 0  # Unique ID assigned to each job from server.
    manager = mp.Manager()
    node_last_seen = manager.dict()  # {node_id: last_seen_time}

    with open(args.node_file) as nodes_ip_file:
        for node_ip in nodes_ip_file:
            ip_address, total_memory = node_ip[:-1].split(',')
            running_jobs[ip_address] = []
            node_list.append(ip_address)
            node_last_seen[ip_address] = None
            compute_nodes[ip_address] = {
                'cpu': None, 'memory': None, 'last_seen': None,
                'total_memory': total_memory,
            }

    process_crash_detector = mp.Process(
        target=detect_node_crash, args=(node_last_seen,))
    process_crash_detector.start()

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
                        if msg.sender == backup_ip:
                            # Heartbeat to backup server includes server state
                            message_handlers.heartbeat_from_backup_handler(
                                compute_nodes=compute_nodes,
                                running_jobs=running_jobs,
                                job_queue=job_queue,
                                job_executable=job_executable,
                                job_sender=job_sender,
                                received_msg=msg)

                        else:
                            message_handlers.heartbeat_handler(
                                compute_nodes=compute_nodes,
                                node_last_seen=node_last_seen,
                                received_msg=msg)

                    elif msg.msg_type == 'JOB_SUBMIT':
                        job_receipt_id += 1
                        message_handlers.job_submit_handler(
                            job_queue=job_queue,
                            compute_nodes=compute_nodes,
                            running_jobs=running_jobs,
                            job_sender=job_sender,
                            job_executable=job_executable,
                            received_msg=msg,
                            job_receipt_id=job_receipt_id)

                    elif msg.msg_type == 'EXECUTED_JOB':
                        global job_queue
                        job_queue = message_handlers.executed_job_handler(
                            job_queue=job_queue,
                            compute_nodes=compute_nodes,
                            running_jobs=running_jobs,
                            job_sender=job_sender,
                            job_executable=job_executable,
                            received_msg=msg)

                    elif msg.msg_type == 'ACK_JOB_EXEC':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_JOB_EXEC_PREEMPT':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'ACK_SUBMITTED_JOB_COMPLETION':
                        message_handlers.ack_ignore_handler()

                    elif msg.msg_type == 'NODE_CRASH':
                        message_handlers.node_crash_handler(
                            received_msg=msg,
                            compute_nodes=compute_nodes,
                            running_jobs=running_jobs,
                            job_queue=job_queue,
                            job_executable=job_executable)

                else:
                    print(sys.stderr, 'closing', client_address,
                          'after reading no data')
                    inputs.remove(msg_socket)
                    msg_socket.close()


if __name__ == '__main__':
    main()

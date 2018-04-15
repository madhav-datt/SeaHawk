"""Script to set up and run central server.

    Responsible for communication with computing nodes, primary backup, job
    scheduling, load balancing, job-node matchmaking decisions etc.

    Messages received from the node:
        - JOB_SUBMIT: The node sends the job to be submitted for execution
            in this message along with the executable file. The server tries to
            schedule the job if possible, else adds it to the job queue.

        - HEARTBEAT: The server(main) receives the cpu-time and memory of the
            node through this heartbeat and also detects that the node is alive.
            It responds with a heartbeat message of its own through a child
            process.

        - EXECUTED_JOB: This message tells the server that the job given to the
            node has either been completed or preempted(with the help of a
            completed flag). If the job has been completed, the server removes
            it from the job_queue and informs the node which has submitted the
            job. Also, it tries to schedule the jobs in the job queue. On the
            other hand, if the job is a preempted one, the server tries to
            schedule it again.

        - ACK_SUBMITTED_JOB_COMPLETION: The server ignores this.

        - ACK_JOB_EXEC: The server ignores this.

        - ACK_JOB_EXEC_PREEMPT: The server ignores this.

    Messages received from backup:
        - HEARTBEAT: Just tells the node that the backup is alive. The serves
            responds with its own heartbeat message in response to this.

    Messages received from its own child process:
        - NODE_CRASH: The server main receives it from a child process after it
            a node or a set of nodes has crashed. The server tries to schedule
            jobs running on those nodes somewhere else.

    Messages sent to node:
        - HEARTBEAT: Server sends this message in response to HEARTBEAT message
            by node. A delay has been set in the server's response, so that
            heartbeat messages do not congest the network.

        - ACK_JOB_SUBMIT: Server sends this message on receiving a JOB_SUBMIT
            message from the node. Includes job's submission id in
            message's content field.

        - ACK_EXECUTED_JOB: Sent in response to EXECUTED_JOB message.

        - JOB_EXEC: Sent by server requesting execution of a job on the node.
            Has job object in content, and executable in file field.

        - JOB_PREEMPT_EXEC: Sent by server requesting preemption of an executing
            job, and execution of a new job. Has (new_job,
            job_to_preempt receipt id) in content, and executable file of new
            job in file.

        - SUBMITTED_JOB_COMPLETION: Server, on receiving EXECUTED_JOB message
            from a node, checks job's 'completed' attribute, and if True,
            sends SUBMITTED_JOB_COMPLETION to submitting node.

    Messages sent to backup:
        - HEARTBEAT: This is sent in response to the heartbeat of the backup.
            The heartbeat has the server state as its content.
"""

import argparse
import multiprocessing as mp
import os
import os.path
import pickle
import select
import socket
import time

from . import message_handlers
from ..messaging import message
from ..messaging import messageutils
from .utils import priorityqueue

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006
BUFFER_SIZE = 1048576
CRASH_ASSUMPTION_TIME = 200  # seconds
CRASH_DETECTOR_SLEEP_TIME = 200  # seconds
SERVER_START_WAIT_TIME = 5  # seconds

BACKUP_SERVER_STATE_PATH = './backup_state.pkl'


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

    compute_nodes = {}  # {node_id: status}
    node_list = []
    job_queue = priorityqueue.JobQueue()
    running_jobs = {}  # {node_id: [list of jobs]}
    job_executable = {}  # {job_id: executable}
    job_sender = {}  # {job_id: sender}

    # In case of backup server taking over on original central server crash
    # gives backup process enough time to terminate
    time.sleep(SERVER_START_WAIT_TIME)

    job_receipt_id = 0  # Unique ID assigned to each job from server.
    manager = mp.Manager()
    node_last_seen = manager.dict()  # {node_id: last_seen_time}

    with open(args.nodes_file) as nodes_ip_file:
        for node_ip in nodes_ip_file:
            ip_address, total_memory = node_ip[:-1].split(',')
            running_jobs[ip_address] = []
            node_list.append(ip_address)
            node_last_seen[ip_address] = time.time()
            compute_nodes[ip_address] = {
                'cpu': None, 'memory': None, 'last_seen': None,
                'total_memory': total_memory,
            }

    # Initialize current server state from backup snapshot
    # Used in case primary backup is taking over as central server
    if os.path.isfile(BACKUP_SERVER_STATE_PATH):
        with open(BACKUP_SERVER_STATE_PATH, 'rb') as backup_server_state:
            server_state = pickle.load(backup_server_state)

        compute_nodes = server_state.compute_nodes
        running_jobs = server_state.running_jobs
        job_sender = server_state.job_sender
        job_executable = server_state.job_executable
        job_queue = server_state.job_queue

        os.remove(BACKUP_SERVER_STATE_PATH)

    process_crash_detector = mp.Process(
        target=detect_node_crash, args=(node_last_seen,))
    process_crash_detector.start()

    # Creates a TCP/IP socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Binds the socket to the port
    server_address = ('', SERVER_RECV_PORT)
    print('Starting up on %s port %s' % server_address)
    server.bind(server_address)
    server.listen(5)

    # Sockets for reading and writing
    inputs = [server]
    outputs = []
    client_address = None

    while inputs:

        # Wait for at least one of the sockets to be ready for processing
        readable, _, _ = select.select(inputs, outputs, inputs)

        # Handle inputs
        for msg_socket in readable:

            if msg_socket is server:
                # A "readable" server socket is ready to accept a connection
                connection, client_address = msg_socket.accept()
                print('New connection from', client_address)
                inputs.append(connection)

            else:
                data = msg_socket.recv(BUFFER_SIZE)
                if data:

                    data_list = []
                    while data:
                        data_list.append(data)
                        data = msg_socket.recv(BUFFER_SIZE)
                    data = b''.join(data_list)

                    msg = pickle.loads(data)
                    assert isinstance(msg, message.Message), \
                        "Received object on socket not of type Message."
                    print(msg)

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
                    print('Closing', client_address, 'after reading no data')
                    inputs.remove(msg_socket)
                    msg_socket.close()


if __name__ == '__main__':
    main()

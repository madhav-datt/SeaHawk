""" Job submission handler, responsible for communication with central server,
    and job submitter.
"""
import sys
import time
import argparse
import pickle
import socket
import os.path
import multiprocessing as mp
from ctypes import c_bool

import message_handlers
import submission_interface
from messaging import message
from messaging import messageutils

CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006
# Buffer size for socket
BUFFER_SIZE = 1048576
# Size of shared memory array
JOB_ARRAY_SIZE = 50
# Time (in seconds) after which it's assumed that server has crashed
CRASH_ASSUMPTION_TIME = 10


def detect_server_crash(last_heartbeat_recv_time):
    """Run as a child process, periodically checking last heartbeat time.

    :param last_heartbeat_recv_time: shared mp.Value, float type
    :return: None

    """
    # When a server crash detected, last_heartbeat_recv_time is
    # set to time.time() + reset_time_allowance, to account for reset time.
    reset_time_allowance = 5.

    # Time to sleep in between checks, adaptive in nature
    sleep_time = 5

    # sleep_time will be set to (1 + sleep_time_additive)* (remaining timeout)
    # should be strictly greater than 0
    sleep_time_additive = 0.1

    while True:
        # Sleep for some time
        time.sleep(sleep_time)

        # Check with last heartbeat time for timeout
        time_since_last_heartbeat = time.time() - last_heartbeat_recv_time
        if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
            # reset last heartbeat time, with reset time allowance
            last_heartbeat_recv_time = time.time() + reset_time_allowance

            # Make and send a crash message to main process which is listening
            # on CLIENT_RECV_PORT for incoming messages
            server_crash_message = message.Message(msg_type='SERVER_CRASH')
            messageutils.send_message(msg=server_crash_message, to='127.0.0.1',
                                      msg_socket=None, port=CLIENT_RECV_PORT)

            # Reset sleep time to the time for reset allowance
            sleep_time = reset_time_allowance
        else:
            # sleep time updated adaptively
            remaining_timeout = \
                CRASH_ASSUMPTION_TIME - time_since_last_heartbeat
            sleep_time = remaining_timeout * (1 + sleep_time_additive)


def main():
    """Get server ip, connect with server, listen for messages, submit jobs

    Job submission is handled entirely by a forked child process.
    Job execution is handed partly by this and a forked child process.
    Heartbeat messages are constantly exchanged.

    """
    # Begin argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("-serverip", help="IP address of central server",
                        type=str, required=True)
    parser.add_argument("-backupip", help="IP address of backup server",
                        type=str)
    args = vars(parser.parse_args())

    # Obtain server and backup ip's from the arguments
    server_ip = args['serverip']
    backup_ip = args['backupip']

    # New stdin descriptor for child process
    newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
    # Shared array of boolean data type with space for JOB_ARRAY_SIZE booleans
    job_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    # Sets book-keeping submitted and acknowledged jobs
    submitted_jobs = set()
    acknowledged_jobs = set()

    # Dict to keep job_receipt_id: pid pairs
    execution_jobs_pid_dict = {}
    num_execution_jobs_recvd = 0

    # Creating new process for job submission interface
    process_submission_interface = mp.Process(
        target=submission_interface.run_submission_interface,
        args=(newstdin, job_array)
    )

    # Starting job submission interface process
    process_submission_interface.start()

    # Shared variable storing time of last heartbeat receipt, of type float
    last_heartbeat_recv_time = mp.Value('d')

    # Creating new process for server crash detection
    process_server_crash_detection = mp.Process(
        target=detect_server_crash, args=(last_heartbeat_recv_time, )
    )

    # Starting server crash detection process
    process_server_crash_detection.start()

    msg_socket = socket.socket()
    msg_socket.bind(('', CLIENT_RECV_PORT))
    msg_socket.listen(5)
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    connection, client_address = msg_socket.accept()
    while True:
        data = connection.recv(BUFFER_SIZE)
        msg = pickle.loads(data)

        # TODO: Add condition: 'and msg.msg_type != 'I AM NEW SERVER' if needed
        if msg.sender != server_ip:
            # Old message from a server detected to have crashed, ignore
            continue

        if msg.msg_type == 'HEARTBEAT':
            last_heartbeat_recv_time = message_handlers.heartbeat_msg_handler(
                job_array, submitted_jobs, server_ip)
        elif msg.msg_type == 'ACK_JOB_SUBMIT':
            message_handlers.ack_job_submit_msg_handler(msg,
                                                        acknowledged_jobs)
        elif msg.msg_type == 'JOB_EXEC':
            num_execution_jobs_recvd += 1
            message_handlers.job_exec_msg_handler(msg, server_ip,
                                                  execution_jobs_pid_dict,
                                                  num_execution_jobs_recvd)
        elif msg.msg_type == 'JOB_PREEMPT':
            message_handlers.\
                job_preemption_msg_handler(msg, execution_jobs_pid_dict,
                                           server_ip)
        elif msg.msg_type == 'SERVER_CRASH':
            server_ip, backup_ip = message_handlers.server_crash_msg_handler(
                server_ip, backup_ip)

        connection.close()


if __name__ == '__main__':
    main()

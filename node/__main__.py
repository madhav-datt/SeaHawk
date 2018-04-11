""" Job submission handler, responsible for communication with central server,
    and job submitter.

    Messages sent to server by parent:
        - JOB_SUBMIT: a job is received on the submission interface, which
            prepares job files, and sets a flag in the shared memory.
            This flag is detected when this main process receives any kind of
            message, and then JOB_SUBMIT is sent to server, with job object
            in content field and executable file in file field of the message.
        
        - HEARTBEAT: When this main process starts, it sends first heartbeat
            to server. After that, it responds with a heartbeat message to
            the server immediately after receiving a heartbeat from server.
            Content of the message contains current system resources' status.
            
        - EXECUTED_JOB: Sent to server on either completing a job execution
            (in this case, sent by the job execution child process), or on
            receiving a JOB_PREEMPT message from server, this main process
            carries out  the preemption procedure, and sends an
            EXECUTED_JOB message to the server, with updated job object
            (job.run_time, job.execution_list, job.completed are updated)
            in the content field.
            On receiving a JOB_PREEMPT for an already preempted job, if
            ACK_EXECUTED_JOB has been received for that job, then the
            duplicate JOB_PREEMPT is ignored, otherwise EXECUTED_JOB message
            is sent again.

        - ACK_SUBMITTED_JOB_COMPLETION: Sent on receiving
            SUBMITTED_JOB_COMPLETION from server. Content field has receipt id.

    Message sent to server by a child:
        - EXECUTED_JOB: As explained before, sent by execute_job() function
            in job_execution.py

    Messages received from child processes:
        - SERVER_CRASH: The child process detects missing heartbeat message
            for a long time, and sends SERVER_CRASH message to this
            main process.

    Messages received from server:
        - HEARTBEAT: Server sends this message in response to HEARTBEAT message
            by node. A delay can/should be there in server's response, so that
            heartbeat messages do not congest the network.

        - ACK_JOB_SUBMIT: Server sends this message on receiving a JOB_SUBMIT
            message from the node. Should include job's submission id in
            message's content field.

        - ACK_EXECUTED_JOB: Sent in response to EXECUTED_JOB message

        - JOB_EXEC: Sent by server requesting execution of a job on the node.
            Should have job object in content, and executable in file field.

        - JOB_PREEMPT: Sent by server requesting preemption of an executing
            job. In

        - SUBMITTED_JOB_COMPLETION: Server, on receiving EXECUTED_JOB message
            from a node, checks job's 'completed' attribute, and if True,
            sends SUBMITTED_JOB_COMPLETION to submitting node

    * TODO
        - When server crash detected, send all non-ack messages
        - For above, need to maintain executed_job msgs sent and acknowledged

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
from messaging import messageutils
from network_params import CLIENT_RECV_PORT, CLIENT_SEND_PORT, BUFFER_SIZE


# Size of shared memory array
JOB_ARRAY_SIZE = 50
# Time (in seconds) after which it's assumed that server has crashed
CRASH_ASSUMPTION_TIME = 10


def detect_server_crash(shared_last_heartbeat_recv_time):
    """Run as a child process, periodically checking last heartbeat time.

    :param shared_last_heartbeat_recv_time: shared mp.Value, float type
    :return: None

    """
    # When a server crash detected, shared_last_heartbeat_recv_time is
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
        time_since_last_heartbeat = \
            time.time() - shared_last_heartbeat_recv_time
        if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
            # reset last heartbeat time, with reset time allowance
            shared_last_heartbeat_recv_time = time.time() + reset_time_allowance

            # Make and send a crash message to main process which is listening
            # on CLIENT_RECV_PORT for incoming messages
            # TODO: Check if 'to' needs to be changed to socket.gethostname()
            messageutils.make_and_send_message(msg_type='SERVER_CRASH',
                                               content=None, file_path=None,
                                               to='127.0.0.1', msg_socket=None,
                                               port=CLIENT_RECV_PORT)

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
    shared_job_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    # Shared arrays for book-keeping submitted, acknowledged & completed jobs
    shared_submitted_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)
    shared_acknowledged_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)
    shared_completed_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    # Dict to keep job_receipt_id: pid pairs
    execution_jobs_pid_dict = {}
    num_execution_jobs_recvd = 0

    # Creating new process for job submission interface
    process_submission_interface = mp.Process(
        target=submission_interface.run_submission_interface,
        args=(newstdin, shared_job_array, shared_submitted_jobs_array,
              shared_acknowledged_jobs_array, shared_completed_jobs_array)
    )

    # Starting job submission interface process
    process_submission_interface.start()

    # Shared variable storing time of last heartbeat receipt, of type float
    shared_last_heartbeat_recv_time = mp.Value('d')

    # Creating new process for server crash detection
    process_server_crash_detection = mp.Process(
        target=detect_server_crash, args=(shared_last_heartbeat_recv_time, )
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
            # Removing pycharm's annoying unused warning for shared variable
            # noinspection PyUnusedLocal
            shared_last_heartbeat_recv_time = \
                message_handlers.heartbeat_msg_handler(
                    shared_job_array, shared_submitted_jobs_array, server_ip)

        elif msg.msg_type == 'ACK_JOB_SUBMIT':
            message_handlers.ack_job_submit_msg_handler(
                msg, shared_acknowledged_jobs_array)

        elif msg.msg_type == 'JOB_EXEC':
            # TODO: See if num_execution_jobs_recvd is useful anywhere
            num_execution_jobs_recvd += 1
            message_handlers.job_exec_msg_handler(msg, server_ip,
                                                  execution_jobs_pid_dict)

        elif msg.msg_type == 'JOB_PREEMPT':
            message_handlers.job_preemption_msg_handler(
                msg, execution_jobs_pid_dict, server_ip)

        elif msg.msg_type == 'SUBMITTED_JOB_COMPLETION':
            message_handlers.submitted_job_completion_msg_handler(
                msg, shared_completed_jobs_array, server_ip)

        elif msg.msg_type == 'SERVER_CRASH':
            server_ip, backup_ip = message_handlers.server_crash_msg_handler(
                server_ip, backup_ip)

        # TODO: Is this correct?
        connection.close()


if __name__ == '__main__':
    main()

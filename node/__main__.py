"""Job submission handler, responsible for communication with central server,
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
            
        - EXECUTED_JOB: Sent to server on either completing a job execution,
            or on receiving a JOB_PREEMPT_EXEC message from server,
            this main process carries out  the preemption procedure, and
            sends an EXECUTED_JOB message to the server, with updated job object
            (job.run_time, job.execution_list, job.completed are updated)
            in the content field.
            On receiving a JOB_PREEMPT_EXEC for an already preempted job, if
            ACK_EXECUTED_JOB has been received for that job, then the
            duplicate JOB_PREEMPT_EXEC is ignored,
            otherwise EXECUTED_JOB message is sent again.

        - ACK_SUBMITTED_JOB_COMPLETION: Sent on receiving
            SUBMITTED_JOB_COMPLETION from server. Content field has receipt id.

        - ACK_JOB_EXEC: Sent on receiving JOB_EXEC from server.

        - ACK_JOB_PREEMPT_EXEC: Sent on receiving JOB_PREEMPT_EXEC from server.


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

        - JOB_PREEMPT_EXEC: Sent by server requesting preemption of an executing
            job, and execution of a new job. Should have (new_job,
            job_to_preempt receipt id) in content, and executable file of new
            job in file.

        - SUBMITTED_JOB_COMPLETION: Server, on receiving EXECUTED_JOB message
            from a node, checks job's 'completed' attribute, and if True,
            sends SUBMITTED_JOB_COMPLETION to submitting node
"""

import argparse
import multiprocessing as mp
import os.path
import pickle
import socket
import signal
import sys
import time
import psutil
from ctypes import c_bool

from . import message_handlers
from . import submission_interface
from ..messaging import message
from ..messaging import messageutils
from ..messaging import network_params

JOB_ARRAY_SIZE = 200  # Size of shared memory array
SERVER_CHANGE_WAIT_TIME = 7


# noinspection PyUnusedLocal
def sigint_handler(signum=signal.SIGINT, frame=None):
    parent_pid = os.getpid()
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    for process in children:
        process.send_signal(signal.SIGTERM)
    sys.exit(0)


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
                        type=str, required=True)
    parser.add_argument("-selfip", help="IP address of self",
                        type=str, required=True)
    args = vars(parser.parse_args())

    # Obtain server and backup ip's from the arguments
    server_ip = args['serverip']
    backup_ip = args['backupip']
    self_ip = args['selfip']

    # New stdin descriptor for child process
    newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
    # Shared array of boolean data type with space for JOB_ARRAY_SIZE booleans
    shared_job_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    # Shared arrays for book-keeping submitted, acknowledged & completed jobs
    shared_submitted_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)
    shared_acknowledged_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)
    shared_completed_jobs_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    manager = mp.Manager()
    # Set-Dict to store all executed and acknowledged executed jobs' receipt ids
    executed_jobs_receipt_ids = manager.dict()
    preempted_jobs_receipt_ids = manager.dict()
    ack_executed_jobs_receipt_ids = manager.dict()
    # Set-Dict to store receipt id of executing jobs
    executing_jobs_receipt_ids = manager.dict()
    # Dict to store execution begin time of executing
    executing_jobs_begin_times = manager.dict()
    # Dict to store execution required time of executing
    executing_jobs_required_times = manager.dict()
    # Dict to keep job_receipt_id: pid pairs
    execution_jobs_pid_dict = manager.dict()
    num_execution_jobs_recvd = 0
    # Dict to store all completed submitted jobs
    submitted_completed_jobs = manager.dict()

    # Bool to store whether submission interface child process has quit
    shared_submission_interface_quit = mp.Value(c_bool, False)
    # Creating new process for job submission interface
    process_submission_interface = mp.Process(
        target=submission_interface.run_submission_interface,
        args=(
            newstdin,
            shared_job_array,
            shared_submitted_jobs_array,
            shared_acknowledged_jobs_array,
            shared_completed_jobs_array,
            executed_jobs_receipt_ids,
            executing_jobs_receipt_ids,
            executing_jobs_begin_times,
            submitted_completed_jobs,
            preempted_jobs_receipt_ids,
            shared_submission_interface_quit,
        )
    )

    # Starting job submission interface process
    process_submission_interface.start()

    # Shared variable storing time of last heartbeat receipt, of type float
    shared_last_heartbeat_recv_time = mp.Value('d', time.time())

    # Mask SIGINT for cleanup with killing all child processes
    # signal.signal(signal.SIGINT, sigint_handler)

    # Start listening to incoming connections on CLIENT_RECV_PORT.
    # Server and child processes connect to this socket
    msg_socket = socket.socket()
    msg_socket.bind(('', network_params.CLIENT_RECV_PORT))
    msg_socket.listen(5)

    # Send first heartbeat to server
    messageutils.send_heartbeat(
        to=server_ip, port=network_params.CLIENT_SEND_PORT)

    while True:
        # Accept an incoming connection
        connection, client_address = msg_socket.accept()

        # Check if user wants to quit
        if shared_submission_interface_quit.value:
            sigint_handler()

        # Receive the data
        data_list = []
        data = connection.recv(network_params.BUFFER_SIZE)
        while data:
            data_list.append(data)
            data = connection.recv(network_params.BUFFER_SIZE)
        data = b''.join(data_list)

        msg = pickle.loads(data)
        assert isinstance(
            msg, message.Message), "Received object on socket not of type " \
                                   "Message."

        if msg.sender == backup_ip and msg.msg_type == 'I_AM_NEW_SERVER':
            # Primary server crash detected by backup server
            # switch primary and backup server ips
            server_ip, backup_ip = backup_ip, server_ip
            time.sleep(SERVER_CHANGE_WAIT_TIME)
            message_handlers.server_crash_msg_handler(
                shared_submitted_jobs_array,
                shared_acknowledged_jobs_array,
                executed_jobs_receipt_ids,
                ack_executed_jobs_receipt_ids,
                server_ip)

        elif msg.sender == backup_ip:
            # Old message from a server detected to have crashed, ignore
            continue

        elif msg.msg_type == 'HEARTBEAT':
            # Removing pycharm's annoying unused warning for shared variable
            # noinspection PyUnusedLocal
            shared_last_heartbeat_recv_time.value = \
                message_handlers.heartbeat_msg_handler(
                    shared_job_array,
                    shared_submitted_jobs_array,
                    executing_jobs_receipt_ids,
                    executed_jobs_receipt_ids,
                    executing_jobs_required_times,
                    executing_jobs_begin_times,
                    execution_jobs_pid_dict,
                    server_ip)

        elif msg.msg_type == 'ACK_JOB_SUBMIT':
            message_handlers.ack_job_submit_msg_handler(
                msg, shared_acknowledged_jobs_array)

        elif msg.msg_type == 'JOB_EXEC':
            # TODO: See if num_execution_jobs_recvd is useful anywhere
            new_job_id = msg.content.receipt_id
            try:
                del executed_jobs_receipt_ids[new_job_id]
            except KeyError:
                pass

            num_execution_jobs_recvd += 1
            message_handlers.job_exec_msg_handler(
                current_job=msg.content,
                job_executable=msg.file,
                execution_jobs_pid_dict=execution_jobs_pid_dict,
                executing_jobs_receipt_ids=executing_jobs_receipt_ids,
                executing_jobs_begin_times=executing_jobs_begin_times,
                executing_jobs_required_times=executing_jobs_required_times,
                executed_jobs_receipt_ids=executed_jobs_receipt_ids,
                shared_submission_interface_quit=shared_submission_interface_quit,
                server_ip=server_ip,
                self_ip=self_ip)
            messageutils.make_and_send_message(
                msg_type='ACK_JOB_EXEC',
                content=None,
                file_path=None,
                to=server_ip,
                msg_socket=None,
                port=network_params.CLIENT_SEND_PORT)

        elif msg.msg_type == 'JOB_PREEMPT_EXEC':
            print(
                'Job Preemption for job r_id =', msg.content[1],
                'received\n\n>>>', end=' ')
            preempted_jobs_receipt_ids[msg.content[1]] = 0
            message_handlers.job_preemption_msg_handler(
                msg=msg,
                execution_jobs_pid_dict=execution_jobs_pid_dict,
                executed_jobs_receipt_ids=executed_jobs_receipt_ids,
                executing_jobs_receipt_ids=executing_jobs_receipt_ids,
                executing_jobs_begin_times=executing_jobs_begin_times,
                executing_jobs_required_times=executing_jobs_required_times,
                shared_submission_interface_quit=shared_submission_interface_quit,
                server_ip=server_ip,
                self_ip=self_ip)
            messageutils.make_and_send_message(
                msg_type='ACK_JOB_PREEMPT_EXEC',
                content=None,
                file_path=None,
                to=server_ip,
                msg_socket=None,
                port=network_params.CLIENT_SEND_PORT)

        elif msg.msg_type == 'EXECUTED_JOB_TO_PARENT':
            message_handlers.executed_job_to_parent_msg_handler(
                msg, executed_jobs_receipt_ids, server_ip)

        elif msg.msg_type == 'ACK_EXECUTED_JOB':
            message_handlers.ack_executed_job_msg_handler(
                msg, ack_executed_jobs_receipt_ids)

        elif msg.msg_type == 'SUBMITTED_JOB_COMPLETION':
            message_handlers.submitted_job_completion_msg_handler(
                msg, shared_completed_jobs_array, submitted_completed_jobs,
                server_ip)

        connection.close()


if __name__ == '__main__':
    main()

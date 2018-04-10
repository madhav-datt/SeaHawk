"""Message handlers for all messages received at a node

Includes handlers for:
    * Heartbeat message from server
    * Acknowledgement of job submission from server
    * Job execution request from server
    * Job preemption request from server
    * Server crash message from child executing crash detection

"""
import os
import time
import errno
import signal
import pickle

from messaging import message
from messaging import messageutils
from job import job_execution
from network_params import CLIENT_SEND_PORT


def heartbeat_msg_handler(shared_job_array, shared_submitted_jobs_array,
                          server_ip):
    """Scan job_array for jobs queued up for submission, send to server.

    :param shared_job_array: shared mp.array, idx set to true if a job idx ever
        requested submission
    :param shared_submitted_jobs_array: shared mp.Array, where idx is set
        to True if job has been submitted to server
    :param server_ip: str, ip address of server
    :return: float, heartbeat receive time

    """
    # Record receive time of heartbeat message
    heartbeat_recv_time = time.time()

    for itr in range(len(shared_job_array)):

        if shared_job_array[itr] and not shared_submitted_jobs_array[itr]:

            # Get job object from pickle
            job_description_filename = './job%s/job.pickle' % itr
            with open(job_description_filename, 'rb') as handle:
                current_job = pickle.load(handle)

            # Get job executable filepath
            job_executable_filename = \
                './job%s/%s' % (itr, current_job.get_executable_name())

            # Make job submission message, with content as current job object,
            # file_path as the executable file path
            job_submission_msg = \
                message.Message('JOB_SUBMIT', content=current_job,
                                file_path=job_executable_filename)

            # Send job submission message to server
            messageutils.send_message(job_submission_msg, to=server_ip,
                                      msg_socket=None, port=CLIENT_SEND_PORT)

            # Update shared_submitted_jobs_array
            shared_submitted_jobs_array[itr] = True
            # TODO: Add log entry here

    # Send heartbeat back to the server
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    return heartbeat_recv_time


def ack_job_submit_msg_handler(msg, shared_acknowledged_jobs_array):
    """Add job to acknowledged job set

    :param msg: message, received message
    :param shared_acknowledged_jobs_array: mp.Array, idx set to true if job
        submission has been acknowledged by server
    :return: None

    """
    ack_job_id = msg.submission_id
    shared_acknowledged_jobs_array[ack_job_id] = True


def job_exec_msg_handler(msg, execution_jobs_pid_dict, num_execution_jobs_recvd,
                         server_ip):
    """Fork a process to execute the job

    :param msg: message, received message of 'JOB_EXEC' msg_type
    :param execution_jobs_pid_dict: dict, storing job_receipt_id:pid pairs
    :param num_execution_jobs_recvd: number of execution job messages received
    :param server_ip: str, ip address of server
    :return: None

    """
    # Get the job object
    current_job = msg.content

    # Make new job directory
    current_job_directory = './execjob%s' % num_execution_jobs_recvd
    if not os.path.exists(current_job_directory):
        os.makedirs(current_job_directory)

    # Store a.out in this directory
    executable_file_bytes = msg.file
    execution_dst = current_job_directory + current_job.get_executable_name()
    with open(execution_dst, 'wb') as file:
        file.write(executable_file_bytes)

    # Fork, and let the child run the executable
    child_pid = os.fork()
    if child_pid == 0:
        # Child process
        job_execution.execute_job(current_job, execution_dst, server_ip)
    else:
        # Parent process
        os.waitpid(child_pid, 0)
        execution_jobs_pid_dict[current_job.receipt_id] = child_pid


def job_preemption_msg_handler(msg, execution_jobs_pid_dict, server_ip):
    """Handle receive of job preemption message

    :param msg: message, received message
    :param execution_jobs_pid_dict: dict, job receipt id:pid pairs
    :param server_ip: str, id address of server
    :return: None

    """
    job_receipt_id = msg.content
    executing_child_pid = execution_jobs_pid_dict[job_receipt_id]

    # Send kill signal to child, which will be handled via sigint_handler
    # sigint_handler will send ack_job_preempt_msg to central server
    try:
        os.kill(executing_child_pid, signal.SIGINT)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH: child process no longer exists
            # Prepare and send acknowledgement message for preemption
            ack_job_preempt_msg = \
                message.Message(msg_type='ACK_JOB_PREEMPT',
                                content='No preemption needed')
            messageutils.send_message(msg=ack_job_preempt_msg, to=server_ip,
                                      msg_socket=None, port=CLIENT_SEND_PORT)

    # Remove key from executing process dict
    del execution_jobs_pid_dict[executing_child_pid]


def job_completion_msg_handler(msg, shared_completed_jobs_array, server_ip):
    """Handle job completion message recvd from server, send ack to server

    :param msg: message, the received message
    :param shared_completed_jobs_array: set, submission ids of all completed
        jobs
    :param server_ip: str, id address of server

    """
    # Get the job object from message's content field
    current_job = msg.content

    # Update the shared_completed_jobs_array
    job_submission_id = current_job.submission_id
    shared_completed_jobs_array[job_submission_id] = True

    # Save the log file in the job's directory in the cwd
    # TODO: Handle directory does not exist condition
    job_directory = './job%s' % job_submission_id
    run_log_file_path = job_directory + '/run_log'
    with open(run_log_file_path, 'wb') as file:
        file.write(msg.file)

    # Prepare and send acknowledgement message for completion message
    ack_job_completion_msg = message.Message(msg_type='ACK_JOB_COMPLETION',
                                             content=current_job.receipt_id)
    messageutils.send_message(msg=ack_job_completion_msg, to=server_ip,
                              msg_socket=None, port=CLIENT_SEND_PORT)


def server_crash_msg_handler(server_ip, backup_ip):
    """Handle a message recvd from server fault detecting child process about
    an assumed server crash at server_ip

    :param server_ip: str, current server's ip, assumed to have crashed
    :param backup_ip: str, backup server's ip
    :return: (str, str), new server ip, new backup ip

    """
    # send first heartbeat to new primary server
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)
    # switch primary and backup server ips in return
    return backup_ip, server_ip

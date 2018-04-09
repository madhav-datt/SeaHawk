"""Message handlers for all messages received at a node

Includes handlers for:
    * Heartbeat message from server
    * Acknowledgement of job submission from server
    * Job execution request from server
    * Job preemption request from server

"""
import os
import time
import errno
import signal
import pickle
import subprocess

from messaging import message
from messaging import messageutils


CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006


def heartbeat_msg_handler(job_array, submitted_jobs, server_ip):
    """Scan job_array for jobs queued up for submission, send to server.

    :param job_array: shared mp.array
    :param submitted_jobs: set, containing submitted job ids
    :param server_ip: str, ip address of server
    :return: float, heartbeat receive time

    """
    # Record receive time of heartbeat message
    heartbeat_recv_time = time.time()

    for itr in range(len(job_array)):

        if job_array[itr] and itr not in submitted_jobs:

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

            # Update submitted_jobs set
            submitted_jobs.add(itr)
            # TODO: Add log entry here

            # Send heartbeat back to the server
            messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    return heartbeat_recv_time


def ack_job_submit_msg_handler(msg, acknowledged_jobs):
    """Add job to acknowledged job set

    :param msg: message, received message
    :param acknowledged_jobs: set, containing acknowledged job ids
    :return: None

    """
    ack_job_id = msg.submission_id
    acknowledged_jobs.add(ack_job_id)


def execute_job(current_job, execution_dst, server_ip):
    """Execute the executable file, and send submission results to server_ip

    :param current_job: job object, to be executed
    :param execution_dst: str, path to executable file
    :param server_ip: str, ip address of server
    :return: None

    """
    # Record start time for job, share the variable with sigint_handler
    start_time = time.time()

    def sigint_handler(signum, frame):
        """Handle sigint signal sent by parent

        Send ack message with updated job runtime to server, and exit.

        :param signum: signal number
        :param frame: frame object
        :return: None

        """
        preemption_end_time = time.time()

        # Update job run time, completion status
        current_job.time_run += (preemption_end_time - start_time)
        if current_job.time_run >= current_job.time_required:
            current_job.completed = True

        # Prepare and send acknowledgement message for preemption
        ack_job_preempt_msg = message.Message(msg_type='ACK_JOB_PREEMPT',
                                              content=current_job)
        messageutils.send_message(msg=ack_job_preempt_msg, to=server_ip,
                                  msg_socket=None, port=CLIENT_SEND_PORT)

        # Gracefully exit
        os._exit(0)

    # Mask the SIGINT signal with sigint_handler function
    signal.signal(signal.SIGINT, sigint_handler)

    # Begin execution
    subprocess.call([execution_dst])
    # Execution call completed
    end_time = time.time()

    # Update job run time
    current_job.time_run += (end_time - start_time)

    # Mark job completion
    current_job.completed = True

    # Prepare and send job completion message to server
    job_completion_msg = message.Message(msg_type='JOB_COMP',
                                         content=current_job)
    messageutils.send_message(msg=job_completion_msg, to=server_ip,
                              msg_socket=None, port=CLIENT_SEND_PORT)


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
    execution_dst = current_job_directory + '/a.out'
    with open(execution_dst, 'wb') as file:
        file.write(executable_file_bytes)

    # Fork, and let the child run the executable
    child_pid = os.fork()
    if child_pid == 0:
        # Child process
        execute_job(current_job, execution_dst, server_ip)
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
    # TODO: Check that job hasn't already completed
    executing_child_pid = execution_jobs_pid_dict[job_receipt_id]

    # Send kill signal to child, which will be handled via sigint_handler
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


def server_crash_msg_handler(server_ip, backup_ip):
    """Handle a message recvd from server fault

    :param server_ip: str, current server's ip, assumed to have crashed
    :param backup_ip: str, backup server's ip
    :return: (str, str), new server ip, new backup ip

    """
    # send first heartbeat to new primary server
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)
    # switch primary and backup server ips in return
    return backup_ip, server_ip

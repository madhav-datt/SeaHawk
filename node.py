""" Job submission handler, responsible for communication with central server,
    and job submitter.
"""
import sys
import argparse
import pickle
import socket
import subprocess
import time
import signal
import errno
import os.path
import multiprocessing as mp
from shutil import copyfile
from ctypes import c_bool

import jobfileparser
import messageutils
import message

CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006
BUFFER_SIZE = 1048576
# Size of shared memory array
JOB_ARRAY_SIZE = 50


def submission_interface(newstdin, job_array):
    """Handle job submission interface.

    Child process forked from main, with shared data array.

    :param newstdin: stdin file descriptor given by parent
    :param job_array: shared boolean array, for signalling submitted jobs to
        parent
    :return: None

    """
    sys.stdin = newstdin
    project_name = 'SeaHawk'
    print('*** Welcome to %s ***\n' % project_name)

    # Keep track of all created jobs, also used to index directory names - which
    # store job files
    num_created_jobs = 0

    print('Enter path address of job description file to submit new job:')
    # keep looping and accepting jobs
    while True:
        jobfile_path = input('\n>>> ')

        # check that entered path is correct and file exists
        if not os.path.isfile(jobfile_path):
            print('Invalid path, file does not exist at given location. Job '
                  'not submitted.')

        else:
            # Parse the job description file, make the job object and store
            # object and executable in a directory
            try:
                current_job = jobfileparser.make_job(jobfile_path)
                num_created_jobs += 1
                current_job.submission_id = num_created_jobs

            except ValueError as job_parse_error:
                print(job_parse_error)
                continue

            # Make an empty directory to store job object pickle and executable
            current_job_directory = './job' + str(num_created_jobs)
            # Race conditions, but not a problem with current application
            if not os.path.exists(current_job_directory):
                os.makedirs(current_job_directory)

            # Make job pickle, and save in the job directory by the name
            # 'job.pickle'
            job_object_path = current_job_directory + '/job.pickle'
            with open(job_object_path, 'wb') as handle:
                pickle.dump(
                    current_job, handle, protocol=pickle.HIGHEST_PROTOCOL)

            # Copy executable to this directory
            job_executable_name = current_job.get_executable_name()
            job_executable_src_path = current_job.executable
            job_executable_dst_path = \
                current_job_directory + job_executable_name
            # IOError possibility, but not a problem with current application
            copyfile(job_executable_src_path, job_executable_dst_path)

            # Set the flag in shared memory
            job_array[num_created_jobs - 1] = True

            print('Job queued for submission.')
            # Continue looping: return to blocking for user input


def heartbeat_msg_handler(job_array, submitted_jobs, server_ip):
    """Scan job_array for jobs queued up for submission, send to server.

    :param job_array: shared mp.array
    :param submitted_jobs: set, containing submitted job ids
    :param server_ip: str, ip address of server
    :return: None

    """
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
            ack_job_preempt_msg = message.Message(msg_type='ACK_JOB_PREEMPT',
                                                 content='No preemption needed')
            messageutils.send_message(msg=ack_job_preempt_msg, to=server_ip,
                                      msg_socket=None, port=CLIENT_SEND_PORT)

    # Remove key from executing process dict
    del execution_jobs_pid_dict[executing_child_pid]


def main():
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
    # Creating shared array of boolean data type with space for JOB_ARRAY_SIZE
    # booleans
    job_array = mp.Array(c_bool, JOB_ARRAY_SIZE)

    # Sets book-keeping submitted and acknowledged jobs
    submitted_jobs = set()
    acknowledged_jobs = set()

    # Dict to keep job_receipt_id: pid pairs
    execution_jobs_pid_dict = {}
    num_execution_jobs_recvd = 0

    # Creating new process for job submission interface
    process_submission_interface = mp.Process(
        target=submission_interface, args=(newstdin, job_array))

    # Starting job submission interface process
    process_submission_interface.start()

    # TODO: Receive acknowledgement and handle
    msg_socket = socket.socket()
    msg_socket.bind(('', CLIENT_RECV_PORT))
    msg_socket.listen(5)
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    connection, client_address = msg_socket.accept()
    while True:
        data = connection.recv(BUFFER_SIZE)
        msg = pickle.loads(data)

        if msg.msg_type == 'HEARTBEAT':
            heartbeat_msg_handler(job_array, submitted_jobs, server_ip)
        elif msg.msg_type == 'ACK_JOB_SUBMIT':
            ack_job_submit_msg_handler(msg, acknowledged_jobs)
        elif msg.msg_type == 'JOB_EXEC':
            num_execution_jobs_recvd += 1
            job_exec_msg_handler(msg, server_ip, execution_jobs_pid_dict,
                                 num_execution_jobs_recvd)
        elif msg.msg_type == 'JOB_PREEMPT':
            job_preemption_msg_handler(msg, execution_jobs_pid_dict, server_ip)

        connection.close()


if __name__ == '__main__':
    main()

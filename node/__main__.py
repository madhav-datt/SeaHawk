""" Job submission handler, responsible for communication with central server,
    and job submitter.
"""
import sys
import argparse
import pickle
import socket
import os.path
import multiprocessing as mp
from shutil import copyfile
from ctypes import c_bool

from job import jobfileparser
import node_message_handlers
from messaging import messageutils

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
            # Continue looping: return to blocking state for user input


def main():
    """Get server ip, connect with server, listen for messages, submit jobs

    Job submission is handled entirely by a forked child process.
    Job execution is handed partly by this and a forked child process.
    Heartbeat messages are constantly exchanged.

    TODO: Server fault detection and switching to backup ip

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
            node_message_handlers.heartbeat_msg_handler(job_array,
                                                        submitted_jobs,
                                                        server_ip)
        elif msg.msg_type == 'ACK_JOB_SUBMIT':
            node_message_handlers.ack_job_submit_msg_handler(msg,
                                                             acknowledged_jobs)
        elif msg.msg_type == 'JOB_EXEC':
            num_execution_jobs_recvd += 1
            node_message_handlers.job_exec_msg_handler(msg, server_ip,
                                                       execution_jobs_pid_dict,
                                                       num_execution_jobs_recvd)
        elif msg.msg_type == 'JOB_PREEMPT':
            node_message_handlers.\
                job_preemption_msg_handler(msg, execution_jobs_pid_dict,
                                           server_ip)

        connection.close()


if __name__ == '__main__':
    main()

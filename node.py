""" Job submission handler, responsible for communication with central server, and job submitter.
"""

import sys
import argparse
import pickle
import socket
import os.path
import multiprocessing as mp
from shutil import copyfile
from ctypes import c_bool

import jobfileparser
import messageutils


CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006
BUFFER_SIZE = 1048576


def submission_interface(newstdin, job_array):
    """
    Handle job submission interface.
    :param newstdin: stdin file descriptor given by parent
    :param job_array: shared boolean array, for signalling subitted jobs to parent
    :return: None
    """

    sys.stdin = newstdin
    project_name = 'SeaHawk'
    print('Welcome to %s\n' % project_name)

    # keep track of all created jobs, also used to index directory names - which store job files
    num_created_jobs = 0

    print('Enter path address of job description file to submit new job:')
    # keep looping and accepting jobs
    while True:
        jobfile_path = input('\n>>> ')

        # check that entered path is correct and file exists
        if not os.path.isfile(jobfile_path):
            print('Invalid path, file does not exist at given location. Job not submitted.')

        else:
            # parse the job description file, make the job object and store object+executable in a directory
            try:
                current_job = jobfileparser.make_job(jobfile_path)
            except ValueError as job_parse_error:
                print(job_parse_error)
                continue

            num_created_jobs += 1
            # make an empty directory to store job object pickle and executable
            current_job_directory = './job' + str(num_created_jobs)
            # race conditions, but not a problem with current application
            if not os.path.exists(current_job_directory):
                os.makedirs(current_job_directory)

            # make job pickle, and save in the job directory by the name 'job.pickle'
            job_object_path = current_job_directory + '/job.pickle'
            with open(job_object_path, 'wb') as handle:
                pickle.dump(current_job, handle, protocol=pickle.HIGHEST_PROTOCOL)

            # copy executable to this directory
            job_executable_name = current_job.get_executable_name()
            job_executable_src_path = current_job.executable
            job_executable_dst_path = current_job_directory + job_executable_name
            # IOError possibility, but not a problem with current application
            copyfile(job_executable_src_path, job_executable_dst_path)

            # set the flag in shared memory
            job_array[num_created_jobs-1] = True

            print('Job queued for submission. Thank you for using %s' % project_name)
            # continue looping: return to blocking for user input


def main():

    # begin argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument("-serverip", help="IP address of central server", type=str)
    args = vars(parser.parse_args())

    # obtain server's ip and port from the arguments
    server_ip = args['serverip']

    # new stdin descriptor for child process
    newstdin = os.fdopen(os.dup(sys.stdin.fileno()))
    # size of shared memory array
    job_array_size = 50
    # creating shared array of boolean data type with space for job_array_size booleans
    job_array = mp.Array(c_bool, job_array_size)

    # creating new process
    process_interface = mp.Process(target=submission_interface, args=(newstdin, job_array))

    # starting process
    process_interface.start()

    # TODO: Receive heartbeats, see shared array for submission and stuff
    msg_socket = socket.socket()
    msg_socket.bind(('', CLIENT_RECV_PORT))
    msg_socket.listen(1)
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    while True:
        connection, client_address = msg_socket.accept()
        data = connection.recv(BUFFER_SIZE)
        msg = pickle.loads(data)

        # TODO: Process message here

        connection.close()


if __name__ == '__main__':
    main()

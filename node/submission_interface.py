"""This module is responsible for handling user interactions for job
    submission and status queries.

    This module's code is run as a forked child of the process running the
    __main__.py module's code.

    It's main job is take input through a prompt, perform command parsing,
    and display corresponding content, and in case of job submission, prepare
    the files for submission and notify parent process through shared memory.
"""

import sys
import os
import pickle
import shutil
import time

from .job import jobfile_parser

JOB_PICKLE_FILE = '/job.pickle'
SUBMITTED_JOB_DIRECTORY_PREFIX = './submit_job'
PROMPT_WELCOME_FILENAME = '/prompt_welcome'


def run_submission_interface(newstdin,
                             shared_job_array,
                             shared_submitted_jobs_array,
                             shared_acknowledged_jobs_array,
                             shared_completed_jobs_array,
                             executed_jobs_receipt_ids,
                             executing_jobs_receipt_ids,
                             executing_jobs_begin_times,
                             submitted_completed_jobs,
                             preempted_jobs_receipt_ids,
                             shared_submission_interface_quit):
    """Handle job submission interface.

    Child process forked from main, with shared data array.

    :param newstdin: stdin file descriptor given by parent.
    :param shared_job_array: shared boolean array, for signalling submitted
        jobs to parent.
    :param shared_submitted_jobs_array: shared mp.Array, where idx is set
        to True if job has been submitted to server.
    :param shared_acknowledged_jobs_array: mp.Array, idx set to true if job
        submission has been acknowledged by server.
    :param shared_completed_jobs_array: mp.Array, idx set to true if job has
        completed execution.
    :param executed_jobs_receipt_ids: set
    :param executing_jobs_receipt_ids: set
    :param executing_jobs_begin_times: dict, receipt id:approx begin time
    :param submitted_completed_jobs: dict
    :param preempted_jobs_receipt_ids: dict
    :param shared_submission_interface_quit: mp.Value c_bool, whether this child
        process has quit.
    :return: None
    """
    sys.stdin = newstdin
    print_welcome_message()

    # Keep track of all created jobs, also used to index directory names - which
    # store job files
    num_created_jobs = 0
    while os.path.isdir(SUBMITTED_JOB_DIRECTORY_PREFIX + str(
            num_created_jobs + 1)):
        num_created_jobs += 1

    # keep looping and accepting jobs
    while True:
        # Take user command input
        command = input('\n>>> ')

        # Get parsed command elements
        command_type, args = command_parser(command)

        # Take action for each type of command
        if command_type == 'EMPTY':
            # Empty string input, continue looping
            continue

        elif command_type == 'HELP':
            # Print help message
            print_help_message()

        elif command_type == 'SUBMIT':
            # Handle job submission
            submission_success = prepare_job_submission(args,
                                                        num_created_jobs,
                                                        shared_job_array)
            if submission_success:
                print('Job queued for submission.')
                num_created_jobs += 1
            else:
                print('Job not submitted.')

        elif command_type == 'HISTORY':
            job_id = int(args[0])
            try:
                print(submitted_completed_jobs[job_id].execution_list)
            except KeyError:
                pass
            finally:
                print('')

        elif command_type == 'STATUS':
            # Print status of all received jobs
            print_status(
                shared_job_array, shared_submitted_jobs_array,
                shared_acknowledged_jobs_array,
                shared_completed_jobs_array,
                executed_jobs_receipt_ids,
                executing_jobs_receipt_ids,
                executing_jobs_begin_times,
                submitted_completed_jobs,
                preempted_jobs_receipt_ids)

        elif command_type == 'QUIT':
            shared_submission_interface_quit.value = True
            # noinspection PyProtectedMember
            os._exit(0)

        elif command_type == 'IMPROPER COMMAND':
            # Print error message for given command
            print_error_message(command)


def print_welcome_message():
    """Print a welcome message read from prompt_welcome file to stdout."""
    prompt_welcome_filepath = \
        os.path.dirname(os.path.realpath(__file__)) + PROMPT_WELCOME_FILENAME
    with open(prompt_welcome_filepath, 'r') as file:
        print(file.read())


def print_error_message(command):
    """Print an error message to stdout in case of improper input command.

    :param command: str, unknown command entered by user.
    """
    print('\n%s: command not found\n'
          'Use "help" to see correct command semantics.' % command)


def print_help_message():
    """Text to display on "help" command.
    """
    print("\nUse:\n"
          "1)\"submit <path_to_jobfile>\" for job submission\n"
          "2)\"status\" to get status of all submitted jobs\n"
          "3)\"quit\" to exit SeaHawk.")


def print_status(shared_job_array,
                 shared_submitted_jobs_array,
                 shared_acknowledged_jobs_array,
                 shared_completed_jobs_array,
                 executed_jobs_receipt_ids,
                 executing_jobs_receipt_ids,
                 executing_jobs_begin_times,
                 submitted_completed_jobs,
                 preempted_jobs_receipt_ids):
    """Print the status of all received jobs to terminal.

    :param shared_job_array: mp.Array of type bool.
    :param shared_submitted_jobs_array: mp.Array of type bool.
    :param shared_acknowledged_jobs_array: mp.Array of type bool.
    :param shared_completed_jobs_array: mp.Array of type bool.
    :param executed_jobs_receipt_ids: set, receipt ids of executed jobs
    :param executing_jobs_receipt_ids: set
    :param executing_jobs_begin_times: dict, receipt id:approx begin time
    :param submitted_completed_jobs: dict
    :param preempted_jobs_receipt_ids: dict
    """
    total_received_jobs = 0

    def yn_map(bool_val):
        return 'Y' if bool_val else 'N'

    print('\nStatus of your jobs')
    print('-' * 20)
    print('%-10s%-15s%-15s%-15s%-20s%-20s' % ('JOB ID', 'SUBMITTED',
                                              'ACKNOWLEDGED', 'COMPLETED',
                                              'COMPLETION TIME',
                                              'RESPONSE_TIME'))

    for id_num in range(len(shared_job_array)):
        if shared_job_array[id_num]:
            total_received_jobs += 1
            try:
                completion_time = \
                    round(submitted_completed_jobs[id_num].
                          submission_completion_time)
            except KeyError:
                completion_time = 'None'
            try:
                response_time = \
                    submitted_completed_jobs[id_num].first_response - \
                    submitted_completed_jobs[id_num].receive_time
            except KeyError:
                response_time = 'None'
            print('%-10s%-15s%-15s%-15s%-20s%-20s'
                  % (id_num,
                     yn_map(shared_submitted_jobs_array[id_num]),
                     yn_map(shared_acknowledged_jobs_array[id_num]),
                     yn_map(shared_completed_jobs_array[id_num]),
                     completion_time,
                     response_time
                     ))

    if total_received_jobs == 0:
        print('%-10s%-15s%-15s%-15s' % ('None', '-', '-', '-'))
    else:
        print('\nTotal received jobs: %d\n' % total_received_jobs)

    # Print status for all the executing jobs
    total_executing_jobs = 0
    print('\nStatus of executing jobs')
    print('-' * 25)

    current_time = time.time()
    print('%-15s%-15s' % ('JOB ID', 'RUN TIME'))

    for id_num in executing_jobs_receipt_ids.keys():
        total_executing_jobs += 1
        if id_num in set(executed_jobs_receipt_ids.keys()):
            print('%-20s%-15s' % (id_num, 'completed'))
        else:
            exec_time = round(current_time - executing_jobs_begin_times[id_num])
            print('%-20s%-15s' % (id_num, exec_time))

    if total_executing_jobs == 0:
        print('%-20s%-15s' % ('None', '-'))
    else:
        print('\nTotal executed/executing jobs: %d\n' % total_executing_jobs)


def command_parser(command):
    """Parse the input command and return type, and any accompanying arguments.

    :param command: str, command input by the user.
    :return: (str, tuple), command type, tuple of accompanying arguments.
    """
    command = command.strip()

    if command == "":
        return 'EMPTY', ()
    elif command == "help":
        return 'HELP', ()
    elif command == "status":
        return 'STATUS', ()
    elif command == "quit":
        return 'QUIT', ()
    elif command.startswith("submit"):
        command_parts = command.split()
        if len(command_parts) != 2:
            return 'IMPROPER COMMAND', ()
        else:
            jobfile_name = command_parts[1]
            return 'SUBMIT', (jobfile_name,)
    elif command.startswith("history"):
        command_parts = command.split()
        if len(command_parts) != 2:
            return 'IMPROPER COMMAND', ()
        else:
            job_id = command_parts[1]
            return 'HISTORY', (job_id,)
    else:
        return 'IMPROPER COMMAND', ()


def prepare_job_submission(command_args, num_created_jobs, shared_job_array):
    """Handle preparation for job submission. Set flag for parent process
    to check and submit the job.

    :param command_args: list, arguments from the command line.
    :param num_created_jobs: int, number of jobs that have already been created.
    :param shared_job_array: mp.Array of type c_bool, idx set to true if job
        with that index has already been submitted.
    :return: bool, true for success, false for error/failure to submit.
    """

    # Handle job submission by reading jobfile path in args
    jobfile_path = command_args[0]
    job_id = num_created_jobs + 1

    # check that entered path is correct and file exists
    if not os.path.isfile(jobfile_path):
        print('Error: No file named %s' % jobfile_path)
        return False
    else:
        # Parse the job description file, make the job object and store
        # object and executable in a directory
        try:
            current_job = jobfile_parser.make_job(jobfile_path)
            current_job.submission_id = job_id
            current_job.submit_time = time.time()
        except ValueError as job_parse_error:
            print(job_parse_error)
            return False

        # Make empty directory to store job object pickle & executable
        current_job_directory = SUBMITTED_JOB_DIRECTORY_PREFIX + str(job_id)
        # Race conditions, but not a problem with current application
        if not os.path.exists(current_job_directory):
            os.makedirs(current_job_directory)

        # Make job pickle, save in the job directory as 'job.pickle'
        job_object_path = current_job_directory + JOB_PICKLE_FILE
        with open(job_object_path, 'wb') as handle:
            pickle.dump(
                current_job, handle, protocol=pickle.HIGHEST_PROTOCOL)

        # Copy executable to this directory
        job_executable_name = current_job.get_executable_name()
        job_executable_src_path = current_job.executable
        job_executable_dst_path = \
            current_job_directory + job_executable_name
        # IOError possible, but not with current application
        shutil.copyfile(job_executable_src_path,
                        job_executable_dst_path)

        # Set the flag in shared memory
        shared_job_array[job_id] = True

        # Return successful submission
        return True

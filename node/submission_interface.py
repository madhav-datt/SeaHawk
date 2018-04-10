import sys
import os
import pickle
import shutil

from job import jobfile_parser


def run_submission_interface(newstdin, shared_job_array,
                             shared_submitted_jobs_array,
                             shared_acknowledged_jobs_array,
                             shared_completed_jobs_array):
    """Handle job submission interface.

    Child process forked from main, with shared data array.

    :param newstdin: stdin file descriptor given by parent
    :param shared_job_array: shared boolean array, for signalling submitted
        jobs to parent
    :param shared_submitted_jobs_array: shared mp.Array, where idx is set
        to True if job has been submitted to server
    :param shared_acknowledged_jobs_array: mp.Array, idx set to true if job
        submission has been acknowledged by server
    :param shared_completed_jobs_array: mp.Array, idx set to true if job has
        completed execution
    :return: None

    """
    sys.stdin = newstdin
    print_welcome_message()

    # Keep track of all created jobs, also used to index directory names - which
    # store job files
    num_created_jobs = 0

    # keep looping and accepting jobs
    while True:
        # Take user command input
        command = input('\n>>> ')

        # Get parsed command elements
        command_type, args = command_parser(command)

        # Take action for each type of command
        if command_type == 'EMPTY':
            continue
        elif command_type == 'HELP':
            # Nothing to be done, continue looping
            continue
        elif command_type == 'SUBMIT':
            # Handle job submission by reading jobfile path in args
            jobfile_path = args[0]

            # check that entered path is correct and file exists
            if not os.path.isfile(jobfile_path):
                print('Invalid job file path, '
                      'file does not exist at given location. '
                      'Job not submitted.')
            else:
                # Parse the job description file, make the job object and store
                # object and executable in a directory
                try:
                    current_job = jobfile_parser.make_job(jobfile_path)
                    num_created_jobs += 1
                    current_job.submission_id = num_created_jobs

                except ValueError as job_parse_error:
                    print(job_parse_error)
                    continue

                # Make empty directory to store job object pickle & executable
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
                # IOError possible, but not with current application
                shutil.copyfile(job_executable_src_path,
                                job_executable_dst_path)

                # Set the flag in shared memory
                shared_job_array[num_created_jobs - 1] = True

                print('Job queued for submission.')
                # Continue looping: return to blocking state for user input

        elif command_type == 'STATUS':
            total_received_jobs = 0

            def yn_map(bool_val):
                return 'Y' if bool_val else 'N'

            print('\n%-10s%-15s%-15s%-15s' % ('JOB ID', 'SUBMITTED',
                                              'ACKNOWLEDGED', 'COMPLETED'))
            for id_num in range(len(shared_job_array)):
                if shared_job_array[id_num]:
                    total_received_jobs += 1
                    print('%-10s%-15s%-15s%-15s'
                          % (id_num+1,
                             yn_map(shared_submitted_jobs_array[id_num]),
                             yn_map(shared_acknowledged_jobs_array[id_num]),
                             yn_map(shared_completed_jobs_array[id_num]),))
            if total_received_jobs == 0:
                print('%-10s%-15s%-15s%-15s' % ('None', '-', '-', '-'))
            else:
                print('\nTotal received jobs: %d\n' % total_received_jobs)


def print_welcome_message():
    """Print a welcome message read from prompt_welcome file to stdout"""
    with open('prompt_welcome', 'r') as file:
        print(file.read())


def print_error_message(command):
    """Print an error message to stdout in case of improper input command

    :param command: str, unknown command entered by user

    """
    print('\n%s: command not found\n'
          'Use "help" to see correct command semantics.' % command)


def print_help_message():
    """Text to display on "help" command"""
    print("\nUse:\n"
          "1)\"submit <path_to_jobfile>\" for job submission\n"
          "2)\"status\" to get status of all submitted jobs")


def command_parser(command):
    """Parse the input command and return type, and any accompanying arguments

    :param command: str, command input by the user
    :return: (str, tuple), command type, tuple of accompanying arguments

    """
    command = command.strip()

    if command == "":
        return 'EMPTY', ()
    elif command == "help":
        print_help_message()
        return 'HELP', ()
    elif command == "status":
        return 'STATUS', ()
    elif command.startswith("submit"):
        command_parts = command.split()
        if len(command_parts) != 2:
            print_error_message(command)
            return 'IMPROPER COMMAND', ()
        else:
            jobfile_name = command_parts[1]
            return 'SUBMIT', (jobfile_name, )
    else:
        print_error_message(command)
        return 'IMPROPER COMMAND', ()
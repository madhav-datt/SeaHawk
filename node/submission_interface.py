import sys
import os
import pickle
import shutil

from job import jobfileparser


def run_submission_interface(newstdin, job_array):
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
            shutil.copyfile(job_executable_src_path, job_executable_dst_path)

            # Set the flag in shared memory
            job_array[num_created_jobs - 1] = True

            print('Job queued for submission.')
            # Continue looping: return to blocking state for user input

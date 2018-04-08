# A parser of Job Description File
import argparse
import os
from job import Job


def parse_jobfile(jobfile_name):
    """
    Parse job text file and return key-value pairs
    :param jobfile_name: str, address of the file
    :return: dict, containing key-value pairs of job fields
    """
    job_description_dict = {'name': 'defaultJob',
                            'executable': None,
                            'priority': 0,
                            'time_required': None,
                            'min_memory': None,
                            'min_cores': None,
                            'max_memory': None}

    with open(jobfile_name, "r") as jobfile:
        for line in jobfile:
            field, value = [val.strip() for val in line.strip().split(':')]
            if field not in job_description_dict:
                raise ValueError("Invalid job description file: Field %s not recognized" % field)
            else:
                job_description_dict[field] = value

    # Check that values for all attributes have been received
    for value in job_description_dict.values():
        if value is None:
            raise ValueError("Invalid job description file: Missing fields found")

    # Check that executable exists
    if not os.path.isfile(job_description_dict['executable']):
        raise ValueError('Invalid job description file: Invalid execution path')

    return job_description_dict


def make_job(jobfile_name):
    """
    Make a job object with attributes as given in the jobfile.
    :param jobfile_name: str, address of jobfile
    :return: job object
    """
    job_description_dict = parse_jobfile(jobfile_name)
    job = Job(**job_description_dict)
    return job


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-filename", help="job descriptor file name", type=str)
    args = vars(parser.parse_args())
    job_filename = args['filename']
    make_job(job_filename)
    print('Job made!')

import os
import time
import signal
import subprocess

from messaging import message
from messaging import messageutils
from network_params import CLIENT_SEND_PORT


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
    job_completion_msg = message.Message(msg_type='EXECUTING_JOB_COMPLETION',
                                         content=current_job)
    messageutils.send_message(msg=job_completion_msg, to=server_ip,
                              msg_socket=None, port=CLIENT_SEND_PORT)

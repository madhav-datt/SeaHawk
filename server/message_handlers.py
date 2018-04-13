"""Message handlers for all messages received at server.

Includes handlers for:
    * Heartbeat message from client/compute nodes.
    * Job submit
"""

import multiprocessing as mp
import time

from . import matchmaking
from .messaging import message
from .messaging import messageutils
from .utils import priorityqueue

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006


def heartbeat_handler(compute_nodes, received_msg):
    """Handler function for HEARTBEAT messages.

    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {node_id: status}
    :param received_msg: message, received message.
    """

    # Update compute node available resources
    if received_msg.sender not in compute_nodes:
        # Node has recovered and needs to be added back to list of nodes
        compute_nodes[received_msg.sender] = {}

    compute_nodes[received_msg.sender]['cpu'] = received_msg['cpu']
    compute_nodes[received_msg.sender]['memory'] = received_msg['memory']
    compute_nodes[received_msg.sender]['last_seen'] = time.time()

    # Send heartbeat message to computing node
    # Creating new process to wait and reply to heartbeat messages
    process_wait_send_heartbeat = mp.Process(
        target=messageutils.wait_send_heartbeat,
        args=(received_msg.sender, SERVER_SEND_PORT, )
    )
    process_wait_send_heartbeat.start()


def job_submit_handler(job_queue,
                       compute_nodes,
                       running_jobs,
                       received_msg,
                       job_sender,
                       job_executable,
                       job_receipt_id):
    """Handler function for JOB_SUBMIT messages.

    :param job_queue: Priority queue for jobs that could not be scheduled.
    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {node_id: status}
    :param running_jobs: Dictionary with jobs running on each system
        {node_id: [list of jobs]}
    :param job_sender: Dictionary with initial sender of jobs {job_id: sender}
    :param job_executable: Dictionary with job executables {job_id: executable}
    :param received_msg: message, received message.
    :param job_receipt_id: int, Unique ID given to the job by server.
    """

    job = received_msg.content
    job.receipt_id = job_receipt_id
    job_sender[job_receipt_id] = received_msg.sender
    job_executable[job_receipt_id] = received_msg.file
    messageutils.make_and_send_message(
        msg_type='ACK_JOB_SUBMIT',
        content=job.submission_id,
        file_path=None,
        to=received_msg.sender,
        port=SERVER_SEND_PORT,
        msg_socket=None)

    schedule_and_send_job(
        job=job,
        executable=received_msg.file,
        job_queue=job_queue,
        compute_nodes=compute_nodes,
        running_jobs=running_jobs)


def executed_job_handler(job_queue,
                         compute_nodes,
                         running_jobs,
                         job_sender,
                         job_executable,
                         received_msg):
    """Handler function for EXECUTED_JOB messages.

    If executed job is complete, tries to schedule all jobs waiting in the
    job_queue. If executed job was preempted, attempts to reschedule it.

    :param job_queue: Priority queue for jobs that could not be scheduled.
    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {node_id: status}
    :param running_jobs: Dictionary with jobs running on each system
        {node_id: [list of jobs]}
    :param job_sender: Dictionary with initial sender of jobs {job_id: sender}
    :param job_executable: Dictionary with job executables {job_id: executable}
    :param received_msg: message, received message.
    :returns job_queue: Priority queue for jobs that have not been scheduled.
    """

    executed_job = received_msg.content
    messageutils.make_and_send_message(
        msg_type='ACK_EXECUTED_JOB',
        content=executed_job.receipt_id,
        file_path=None,
        to=received_msg.sender,
        port=SERVER_SEND_PORT,
        msg_socket=None)
    running_jobs[received_msg.sender].remove(executed_job)

    if executed_job.complete:

        # Send completion result to initial node where job was created.
        completed_job_msg = message.Message(
            msg_type='SUBMITTED_JOB_COMPLETION', content=executed_job)
        messageutils.send_message(
            msg=completed_job_msg,
            to=job_sender[executed_job.receipt_id],
            port=SERVER_SEND_PORT)

        # Schedule jobs waiting in job_queue

        # List of jobs in job_queue that could not be scheduled
        wait_queue = priorityqueue.JobQueue()
        while not job_queue.empty():
            job = job_queue.get()
            schedule_and_send_job(
                job=job,
                executable=job_executable[job.receipt_id],
                job_queue=wait_queue,
                compute_nodes=compute_nodes,
                running_jobs=running_jobs)

        job_queue = wait_queue

    # Preempted job has been returned for rescheduling
    else:
        schedule_and_send_job(
            job=executed_job,
            executable=job_executable[executed_job.receipt_id],
            job_queue=job_queue,
            compute_nodes=compute_nodes,
            running_jobs=running_jobs)

    return job_queue


def ack_ignore_handler():
    """Handler function for ACK messages. Ignores received ack messages.
    """
    pass


# Helper functions

def schedule_and_send_job(job,
                          executable,
                          job_queue,
                          compute_nodes,
                          running_jobs):
    """Schedule and send job to target node for execution.

    Tries to schedule job, and sends to selected computing node. If scheduling
    not possible, adds to job queue for scheduling later.

    :param job: Job object for job to be scheduled.
    :param executable: Bytes for job executable.
    :param job_queue: Priority queue for jobs that could not be scheduled.
    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {node_id: status}
    :param running_jobs: Dictionary with jobs running on each system
        {node_id: [list of jobs]}
    """

    node_for_job, preempt_job = matchmaking.matchmaking(
        job=job, compute_nodes=compute_nodes, running_jobs=running_jobs)

    # Job cannot be scheduled at the moment
    if node_for_job is None:
        job_queue.put(job)
        return

    if preempt_job is not None:
        job_exec_msg = message.Message(
            msg_type='JOB_PREEMPT_EXEC',
            content=(job, preempt_job.receipt_id),
            file=executable)
    else:
        job_exec_msg = message.Message(
            msg_type='JOB_EXEC', content=job, file=executable)

    messageutils.send_message(
        msg=job_exec_msg, to=node_for_job, port=SERVER_SEND_PORT)

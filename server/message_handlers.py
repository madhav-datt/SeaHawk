"""Message handlers for all messages received at server

Includes handlers for:
    * Heartbeat message from server
"""

import time

from . import matchmaking
from .messaging import message
from .messaging import messageutils

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006


def heartbeat_handler(compute_nodes, received_msg):
    """Handler function for HEARTBEAT messages.

    :param compute_nodes:
    :param received_msg: message, received message.
    """

    # Update compute node available resources
    compute_nodes[received_msg.sender]['cpu'] = received_msg['cpu']
    compute_nodes[received_msg.sender]['memory'] = received_msg['memory']
    compute_nodes[received_msg.sender]['last_seen'] = time.time()

    # Send heartbeat message to computing node
    messageutils.send_heartbeat(to=received_msg.sender, port=SERVER_SEND_PORT)


def job_submit_handler(job_queue,
                       compute_nodes,
                       running_jobs,
                       received_msg,
                       job_receipt_id):
    """Handler function for JOB_SUBMIT messages.

    :param job_queue: Priority queue for jobs that could not be scheduled.
    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {nodeId: status}
    :param running_jobs: Dictionary with jobs running on each system
        {nodeId: [list of jobs]}
    :param received_msg: message, received message.
    :param job_receipt_id: int, Unique ID given to the job by server.
    """

    job = received_msg.content
    job.receipt_id = job_receipt_id

    node_for_job, preempt_job = matchmaking.matchmaking(
        job=job, compute_nodes=compute_nodes, running_jobs=running_jobs)

    if node_for_job is None:
        job_queue.put(job)
        return

    if preempt_job is not None:
        job_exec_msg = message.Message(
            msg_type='JOB_PREEMPT_EXEC',
            content=(job, preempt_job.receipt_id),
            file=received_msg.file)

    else:
        job_exec_msg = message.Message(
            msg_type='JOB_EXEC', content=job, file=received_msg.file)

    messageutils.send_message(
        msg=job_exec_msg, to=node_for_job, port=SERVER_SEND_PORT)

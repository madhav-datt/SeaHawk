"""Message handlers for all messages received at server

Includes handlers for:
    * Heartbeat message from server
"""

from server.messaging import messageutils
import time

SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006


def heartbeat_handler(compute_nodes, received_msg):
    """Handler function for HEARTBEAT messages.

    :param received_msg: message, received message.
    """

    compute_nodes[received_msg.sender]['cpu'] = received_msg['cpu']
    compute_nodes[received_msg.sender]['memory'] = received_msg['memory']
    compute_nodes[received_msg.sender]['last_seen'] = time.time()

    # Send heartbeat message to computing node
    messageutils.send_heartbeat(to=received_msg.sender, port=SERVER_SEND_PORT)


def job_submit_handler(received_msg):
    """Handler function for JOB_SUBMIT messages.

    :param received_msg: message, received message.
    """

    pass

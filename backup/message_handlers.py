"""Message handlers for all messages received at backup.

Includes handlers for:
    * Heartbeat message from server.
    * Notification about server crash.
"""

import os
import pickle
import sys

from ..messaging import messageutils

CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006

# Only in case backup takes over as central server
SERVER_SEND_PORT = 5005
SERVER_RECV_PORT = 5006

BACKUP_SERVER_STATE_PATH = './backup_state.pkl'


def heartbeat_handler(received_msg):
    """Handler function for HEARTBEAT messages from server.

    Saves ServerState to pickle file for use when backup needs to take over as
    central server.

    :param received_msg: message, received message.
    :return: ServerState object received from central server.
    """

    messageutils.send_heartbeat(to=received_msg.sender, port=CLIENT_SEND_PORT)
    server_state = received_msg.content
    with open(BACKUP_SERVER_STATE_PATH, 'wb') as server_state_file:
        pickle.dump(server_state, server_state_file)

    return server_state


def server_crash_handler(server_state, crashed_server_ip, backup_ip,
                         nodes_file_path):
    """Handler function for SERVER_CRASH messages from crash detector.

    Informs all computing nodes about new server, takes over as central server,
    and kills backup process.

    :param server_state: ServerState with last known state of central server.
    :param crashed_server_ip: String with IP address of crashed central server.
    :param backup_ip: String with IP address of primary backup (this node).
    :param nodes_file_path: Absolute path to txt file with IP address, total
        memory of each client/computing node.
    """

    for node_id, status in server_state.computing_nodes.items():
        messageutils.make_and_send_message(
            msg_type='I_AM_NEW_SERVER',
            content=None,
            file_path=None,
            to=node_id,
            msg_socket=None,
            port=SERVER_SEND_PORT)

    start_server_command = (
        'python -m server --server-ip {server_ip} --backup-ip {backup_ip} '
        '--nodes-file {nodes_file}'.format(
            server_ip=backup_ip,
            backup_ip=crashed_server_ip,
            nodes_file=nodes_file_path))
    os.system(start_server_command)
    sys.exit()

"""Script to set up and run primary backup.

    Responsible for communication with central server, detecting server failure,
    maintaining backup of server states, and taking over if server fails.

    Messages received from central server:
        - HEARTBEAT: Tells the primary backup that the central server is alive.

        - BACKUP_UPDATE: Includes ServerState object with state of server to be
            backed up and used in case of central server crash.

    Messages received from its own child process:
        - SERVER_CRASH: The backup main receives it from a child process after
            the child detects the central server to have crashed. Backup takes
            over, informs all compute nodes about the new server, and restores
            server state on current node from ServerState received in last
            heartbeat message.

    Messages sent to central server:
        - HEARTBEAT: Just tells the node that the backup is alive. Sent to the
            central server in response to its own heartbeat message.

    Messages sent to computing nodes:
        - I_AM_NEW_SERVER: Sent to all nodes when the backup detects that the
            original central server has crashed, and backup takes over. Backup
            process exits, and starts server process on current node.
"""

import argparse
import multiprocessing as mp
import pickle
import socket
import time

from . import message_handlers
from ..messaging import message
from ..messaging import messageutils

CLIENT_RECV_PORT = 5005
CLIENT_SEND_PORT = 5006
BUFFER_SIZE = 1048576
CRASH_ASSUMPTION_TIME = 20  # seconds
CRASH_DETECTOR_SLEEP_TIME = 5  # seconds


def detect_server_crash(server_last_seen_time, backup_ip):
    """Detects central server crashes.

    Run as a child process, periodically checking last heartbeat times for each
    computing node.

    :param server_last_seen_time: Float with time when last heartbeat was
        received from central server.
    :param backup_ip: String with IP address of backup server (this node).
    """

    while True:
        time.sleep(CRASH_DETECTOR_SLEEP_TIME)
        print('CHECKING CRASH')

        current_time = time.time()
        time_since_last_heartbeat = current_time - server_last_seen_time.value
        if time_since_last_heartbeat > CRASH_ASSUMPTION_TIME:
            print('NODE CRASHED BACKUP')

            # Make and send a crash message to main process which is listening
            # on SERVER_RECV_PORT for incoming messages.
            messageutils.make_and_send_message(msg_type='SERVER_CRASH',
                                               content=None,
                                               file_path=None,
                                               to=backup_ip,
                                               msg_socket=None,
                                               port=CLIENT_RECV_PORT)


def main():
    parser = argparse.ArgumentParser(description='Set up central server.')
    parser.add_argument(
        '--server-ip',
        required=True,
        help='IP address of central server.')
    parser.add_argument(
        '--backup-ip',
        required=True,
        help='IP address of primary backup server (this node).')
    args = parser.parse_args()

    server_ip = args.server_ip
    backup_ip = args.backup_ip
    server_state = None

    # Shared variable storing time of last heartbeat receipt, of type float
    shared_last_heartbeat_recv_time = mp.Value('d', time.time())

    # Creating new process for server crash detection
    process_server_crash_detection = mp.Process(
        target=detect_server_crash,
        args=(shared_last_heartbeat_recv_time, backup_ip, )
    )
    process_server_crash_detection.daemon = 1
    process_server_crash_detection.start()

    # Start listening to incoming connections on CLIENT_RECV_PORT.
    # Server and child processes connect to this socket
    msg_socket = socket.socket()
    msg_socket.bind(('', CLIENT_RECV_PORT))
    msg_socket.listen(5)

    # Send first heartbeat to server
    messageutils.send_heartbeat(to=server_ip, port=CLIENT_SEND_PORT)

    while True:
        # Accept an incoming connection
        connection, client_address = msg_socket.accept()

        # Receive the data
        data_list = []
        data = connection.recv(BUFFER_SIZE)
        while data:
            data_list.append(data)
            data = connection.recv(BUFFER_SIZE)
        data = b''.join(data_list)

        msg = pickle.loads(data)
        assert isinstance(msg, message.Message), "Received object on socket " \
                                                 "not of type Message."
        print(msg)

        if msg.msg_type == 'HEARTBEAT':
            shared_last_heartbeat_recv_time.value = time.time()
            message_handlers.heartbeat_handler(received_msg=msg)

        elif msg.msg_type == 'BACKUP_UPDATE':
            server_state = message_handlers.backup_update_handler(
                previous_server_state=server_state,
                received_msg=msg)

        elif msg.msg_type == 'SERVER_CRASH':
            message_handlers.server_crash_handler(
                server_state=server_state,
                crashed_server_ip=server_ip,
                backup_ip=backup_ip,
                child_pid=process_server_crash_detection.pid,
                socket_to_close=msg_socket)


if __name__ == '__main__':
    main()

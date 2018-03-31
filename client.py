import socket

BUFFER_SIZE = 1024


def send_message(to, port, message_type, content=None, file=None):
    """

    :param to:
    :param port:
    :param message_type:
    :param content:
    :param file:
    """

    msg_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    msg_socket.connect((to, port))



    f = open('TT.png','rb')

    l = f.read(1024)
    while (l):
        msg_socket.send(l)
        l = f.read(1024)
    f.close()

    msg_socket.shutdown(socket.SHUT_WR)
    print msg_socket.recv(1024)
    msg_socket.close()
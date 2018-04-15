"""ServerState class implementation for state of server to be sent to backup.
"""


class ServerState(object):
    """Maintains current state/snapshot of information known to server.
    """

    def __init__(self, compute_nodes, running_jobs, job_queue, job_executable,
                 job_sender):
        """Initializes ServerState class object.

        :param job_queue: Priority queue for jobs that could not be scheduled.
        :param compute_nodes: Dictionary with cpu usage and memory of each node
            {node_id: status}
        :param running_jobs: Dictionary with jobs running on each system
            {node_id: [list of jobs]}
        :param job_sender: Dictionary with initial sender of jobs
            {job_id: sender}
        :param job_executable: Dictionary with job executables
            {job_id: executable}
        """

        self.job_queue = job_queue
        self.compute_nodes = compute_nodes
        self.running_jobs = running_jobs
        self.job_sender = job_sender
        self.job_executable = job_executable

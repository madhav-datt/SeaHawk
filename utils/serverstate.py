"""ServerState class implementation for state of server to be sent to backup.
"""


class ServerState(object):
    """Maintains current state/snapshot of information known to server.

    Args:
        job_queue: List of jobs that could not be scheduled.
        compute_nodes: Dictionary with cpu usage and memory of each node
            {node_id: status}
        running_jobs: Dictionary with jobs running on each system
            {node_id: [list of jobs]}
        job_sender: Dictionary with initial sender of jobs {job_id: sender}
        job_executable: Dictionary with job executables {job_id: executable}
        state_order: Integer with sequence ordering number of ServerState
            sent to backup server.
    """

    def __init__(self, compute_nodes, running_jobs, job_queue, job_executable,
                 job_sender, state_order):
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
        :param state_order: Integer with sequence ordering number of ServerState
            sent to backup server.
        """

        # Convert from priority queue class JobQueue to a simple list
        self.state_order = state_order
        self.job_queue = []
        while not job_queue.empty():
            job = job_queue.get()
            self.job_queue.append(job)

        self.compute_nodes = compute_nodes
        self.running_jobs = running_jobs
        self.job_sender = job_sender
        self.job_executable = job_executable

    def __str__(self):
        """Custom function to print ServerState details.

        :return: String representation of ServerState object.
        """
        return 'SERVER STATE: ' + str(self.compute_nodes)

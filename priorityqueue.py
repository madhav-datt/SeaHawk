"""Custom PriorityQueue implementation for job scheduling queues.
"""

import queue


class JobQueue(queue.PriorityQueue):
    """Reversed priority queue class to handle job objects scheduling.
    """

    def put(self, job_object, block=True, timeout=None):
        job_tuple = job_object.priority * -1, job_object
        queue.PriorityQueue.put(self, job_tuple, block, timeout)

    def get(self, block=True, timeout=None):
        _, job_tuple = queue.PriorityQueue.get(self, block, timeout)
        return job_tuple

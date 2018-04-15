"""File which does the matchmaking of jobs.
"""


def matchmaking(job, compute_nodes, running_jobs):
    """Matchmaking algorithm to pick best compute node for a job.

    First find candidate machines (min requirement of memory and CPU).
    If a candidate is idle, order by max memory.
    If no idle candidate, order by CPU usage.
    If CPU usage of all candidates > 80%, need to preempt.

    :param job: The job to be scheduled
    :param compute_nodes: Dictionary with cpu usage and memory of each node
        {node_id: status}
    :param running_jobs: Dictionary with jobs running on each system
        {node_id: [list of jobs]}
    :return: (node, job). The node where this job has to be scheduled, and also
        the job that is to be preempted (if any)
    """

    candidates = []
    best_candidate = None

    # Find the set of probable candidates
    for node_id, status in compute_nodes.items():
        if status['memory'] >= job.min_memory and status['cpu'] > 20:
            candidates.append(node_id)

    # Try to assign a node for the job from the probable candidates
    if len(candidates) > 0:
        # Find the set of idle machines
        idle_machines = []
        for candidate in candidates:
            if len(running_jobs[candidate]) == 0:
                idle_machines.append(candidate)

        # Choose one of the idle machines based on max memory preference
        if len(idle_machines) > 0:
            # Order by max memory
            diff_from_max = float('inf')
            for idle_machine in idle_machines:
                memory_diff = abs(
                    job.max_memory - compute_nodes[idle_machines]['memory'])
                if memory_diff < diff_from_max:
                    diff_from_max = memory_diff
                    best_candidate = idle_machine
            running_jobs[best_candidate].append(job)
            return best_candidate, None

        else:
            # Order by cpu usage in case no machine is idle
            min_cpu_usage = 100
            for idle_machine in idle_machines:
                if compute_nodes[idle_machine]['cpu'] < min_cpu_usage:
                    min_cpu_usage = compute_nodes[idle_machine]['cpu']
                    best_candidate = idle_machine
            running_jobs[best_candidate].append(job)
            return best_candidate, None

    else:
        # Preemption is needed, so first first the lowest priority job on each
        # machine
        lowest_priority_jobs = {}
        for node_id, job_list in running_jobs.items():
            lowest_priority_jobs[node_id] = min(
                job_list, key=lambda j: j.priority)

        # Out of all the low priority jobs on each machine,
        # Preempt the lowest priority one which satisfies memory constraints

        job_to_preempt = None
        for node_id, lowest_priority_job in lowest_priority_jobs.items():
            if (job.min_memory <
                    compute_nodes[node_id]['memory'] +
                    lowest_priority_job.min_memory):
                if (job_to_preempt is None) or (
                        job_to_preempt.priority > lowest_priority_job.priority):
                    job_to_preempt = lowest_priority_job
                    best_candidate = node_id

        if best_candidate is not None:
            running_jobs[best_candidate].append(job)
        return best_candidate, job_to_preempt

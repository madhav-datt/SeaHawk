import job


# compute_nodes: stores cpu usage and memory of each node {nodeId: status}
# running_jobs: stores jobs running on each system {nodeId: [list of jobs]}

#TODO: initialize it somewhere
running_jobs = {}


def matchmaking(job,compute_nodes):
    # First find candiate machines (min requirement of memory and CPU)
    #TODO: If no candidate is available, need to preempt
    #TODO: If a candidate is idle, order by max memory
    #TODO: If no idle candidate, order by CPU usage
    #TODO: If CPU usage of all candidates > 80%, need to preempt

    candidates = []
    best_candidate = None

    for node_id,status in compute_nodes.items():
        if status[memory] >= job.min_memory and status[cpu]<80:
            candidates.append(node_id)

    if(len(candidates) > 0):
        # try to schedule the job
        idle_machines = []
        for candidate in candidates:
            if len(running_jobs[candidate]) == 0:
                idle_machines.append(candidate)

        if len(idle_machines) > 0:
            # order by max memory
            diff_from_max = float('inf')
            for idle_machine in idle_machines:
                memory_diff = abs(job.max_memory - compute_nodes[idle_machines][memory])
                if memory_diff < diff_from_max:
                    diff_from_max = memory_diff
                    best_candidate = idle_machine

            return best_candidate

        else:
            # order by cpu usage
            min_cpu_usage = 100

            for idle_machine in idle_machines:
                if compute_nodes[idle_machine][cpu] < min_cpu_usage:
                    min_cpu_usage = compute_nodes[idle_machine][cpu]
                    best_candidate = idle_machine

            return best_candidate

    else:
        # preemption is needed
        print('Do something')
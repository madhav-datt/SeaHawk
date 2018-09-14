# SeaHawk
SeaHawk :bird: is a simple distributed cluster middleware system, like [Google Borg](https://pdos.csail.mit.edu/6.824/papers/borg.pdf) and 
[Condor](http://research.cs.wisc.edu/htcondor/doc/beowulf-chapter-rev1.pdf) but
much much smaller.

## Feature Set
### Architecture/Model

* A Central Server/Master Scheduler node exists, and has information about user priority
values (default value is assigned for unknown users), and attributes (RAM, disk, processor
information) for all machines. A fully connected topology is used.

* Backup servers exist for single point failure at the central manager. Heartbeat messages
are sent periodically to the backups, along with update messages on any state change at
the central manager. Backups update and send back ack message, and the central manager
can perform its next operation on receiving an ack from all backup nodes.

* Distributed job submission: Jobs may be submitted to any node in the system. The node then sends it to the central node for management. The central node replies with an acknowledgement.

* A log file is created and maintained for each job, tracking the nodes on which the job has
previously been executed, preemption details, number of time quanta for which it had access
to computing resources etc.

* Heartbeat messages: At fixed time intervals, the central node sends a heartbeat message to
all nodes. On receiving this message, each node sends a reply.

### Scheduling and Load Balancing
#### Job Submission

* Each job is submitted along with a Job Submission File, which contains the following information: user name, job name, job priority, resource requirements (memory, storage, CPU specifications, GPU, system architecture), executable details and paths (may be multiple independent tasks, each with a separate executable file), execution time required, system
preferences (beyond the hard requirements), dependencies/libraries needed for system setup
before job can be executed in the form of a shell script for setup, and input files (if any).

#### Matchmaking and Scheduling Policy

* Priority queue of jobs is maintained at the central node (ordered by user priority and then
job priority). Matchmaking first identifies candidate machines using requirement matching.
Ranking among candidates is done by computing a scheduling cost for each candidate, which
is composed of user preference, requirement for preemption of executing jobs, current system
load, and spread of different priority jobs over machines. Ties are broken randomly.

* If all candidates involve preempting a higher priority process, the job will be moved to a
wait queue. The jobs in the wait queue are moved to the priority queue on receiving any
job completion at the central node.

#### Job Preemption and Migration

* On the requirement for a job preemption at a machine, lowest priority job is preempted after
it has run for a fixed time quanta (starting from its execution at this machine). Preempted
jobs are sent back to the central node for rescheduling.

* Priority of a job in the priority queue is incremented if its waiting time exceeds a threshold.

* A completed job is sent back to the central server. The central server acknowledges it after
receiving reply from all the backups, and then (if needed) central sends the files and logs
to the node on which the job was submitted by the client.

### Fault Tolerance
#### Machine Failure

* Node failure: If heartbeat messages from the central server do not receive a reply from a
node after a certain fixed time, the node is assumed to have crashed. The central server
removes it from the list of active machines, and adds all the jobs sent to it to the priority
queue.

* Central Server Failure: On not receiving a heartbeat message after a fixed time, any backup
server may assume that the central node has failed and start a leader election process. The
new elected leader broadcasts a message to inform all computing nodes about this change.
All nodes make a note of it.

* Nodes resend completion messages to the central server, for all the jobs that were completed
earlier but no ack message was received from the old central server (that is, they were
completed after the central node crashed).

* Nodes resend new jobs to the central server, for all jobs that were submitted at those nodes
and were forwarded to the central server, but no ack was received (that is, they were sent
after the central node crashed).

#### Machine Recovery

* Node Recovery: It sends an alive message to all the backups, the current leader replies and
also makes a note that this node is active now(all other backups also make a note of this).
As the node is aware of the leader now, it starts functioning as a normal node.

* Old Central Node/ Backup recovery: It also sends an alive message to all the backups, the
current leader replies, updates its view(all other backups also update their view), and before
doing anything else makes sure that the state of this recovered backup is consistent with
the current state of the system.

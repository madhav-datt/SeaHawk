class Job:
	def __init__(self, name, executable, priority, time_required, min_memory, min_cores, max_memory):
		"""
		:param name: str, name of the job
		:param executable: str, address of the executable file
		:param priority:  int, -20 to 20 denoting priority
		:param time_required: int, seconds of time required
		:param min_memory: int, min amount of memory (in MB) required for execution
		:param min_cores: int, min no. of cores required for execution
		:param preference: int, max amount of memory (in MB) preferred for execution
		"""
		self.name = name
		self.username = os.uname()[1]
		self.executable = executable
		self.priority = priority
		self.time_required = time_required
		self.time_run = 0
		# Requirements
		self.min_memory = min_memory
		self.min_cores = min_cores
		self.max_memory = p
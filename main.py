import random
from collections import defaultdict
import heapq

# Job structure
class Job:
    def __init__(self, job_id, machine_type, processing_time, changeover_time):
        self.id = job_id
        self.machine_type = machine_type
        self.processing_time = processing_time
        self.changeover_time = changeover_time

# Generate sample jobs
def generate_jobs(num_jobs=40):
    jobs = []
    for i in range(num_jobs):
        if i % 5 == 0:
            machine = 'Jacquard'
            base_time = 18
            utilization = 0.53
        elif i % 3 == 0:
            machine = 'Wide300'
            base_time = 10
            utilization = 0.95
        else:
            machine = 'Narrow140'
            base_time = 6
            utilization = 0.62

        proc_time = int(base_time + random.uniform(0, 4))  # variable job time
        changeover_time = 3 if random.random() < 0.8 else 24
        jobs.append(Job(f"J{i+1}", machine, proc_time, changeover_time))
    return jobs

# Schedule jobs on a single machine (simple EDD heuristic)
def schedule_machine(machine_jobs):
    machine_jobs.sort(key=lambda job: job.processing_time + job.changeover_time)
    schedule = []
    current_time = 0
    for job in machine_jobs:
        start = current_time + job.changeover_time
        end = start + job.processing_time
        schedule.append((job.id, start, end))
        current_time = end
    return schedule, current_time

# Main SBH function
def shifting_bottleneck_heuristic(jobs):
    # Group jobs by machine type
    machine_jobs = defaultdict(list)
    for job in jobs:
        machine_jobs[job.machine_type].append(job)

    final_schedule = {}
    unscheduled_machines = list(machine_jobs.keys())

    while unscheduled_machines:
        machine_makespans = {}
        machine_schedules = {}
        
        # Schedule each unscheduled machine and get makespan
        for machine in unscheduled_machines:
            sched, makespan = schedule_machine(machine_jobs[machine])
            machine_makespans[machine] = makespan
            machine_schedules[machine] = sched

        # Pick bottleneck machine (max makespan)
        bottleneck = max(machine_makespans, key=machine_makespans.get)
        final_schedule[bottleneck] = machine_schedules[bottleneck]
        unscheduled_machines.remove(bottleneck)

    return final_schedule

# Example usage
jobs = generate_jobs()
schedule = shifting_bottleneck_heuristic(jobs)

# Print final schedule
for machine, sched in schedule.items():
    print(f"\nMachine: {machine}")
    for job_id, start, end in sched:
        print(f"  {job_id}: Start at {start}h, End at {end}h")

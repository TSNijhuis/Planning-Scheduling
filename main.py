import random
from collections import defaultdict
import copy
import numpy as np  # <-- Add this import

# Job structure
class Job:
    def __init__(self, job_id, machine_type, processing_time, changeover_time, due_date):
        self.id = job_id
        self.machine_type = machine_type
        self.processing_time = processing_time
        self.changeover_time = changeover_time
        self.due_date = due_date

# Use capacity-based distribution for job generation
def generate_jobs():
    # Probabilities from your analysis
    probs = [0.36, 0.564, 0.075]
    machine_types = ['300 cm', '140 cm', 'Jacquard']
    total_jobs = 40

    # Multinomial distribution for job counts
    job_counts = np.random.multinomial(total_jobs, probs)
    job_distribution = dict(zip(machine_types, job_counts))

    base_times = {
        '300 cm': 10,
        '140 cm': 6,
        'Jacquard': 18
    }

    jobs = []
    job_count = 1

    for machine_type, num_jobs in job_distribution.items():
        for _ in range(num_jobs):
            base_time = base_times[machine_type]
            proc_time = int(random.triangular(4, base_time, base_time + 5))
            changeover_time = 3 if random.random() < 0.8 else 24
            due_date = int(proc_time + random.gauss(6, 2))
            due_date = max(due_date, proc_time + 1)
            jobs.append(Job(f"J{job_count}", machine_type, proc_time, changeover_time, due_date))
            job_count += 1

    # Print the actual distribution for transparency
    print("Job distribution this run:", job_distribution)
    return jobs

# EDD or SPT+changeover scheduling
def schedule_machine(machine_jobs, heuristic='EDD'):
    if heuristic == 'EDD':
        machine_jobs.sort(key=lambda job: job.due_date)
    else:
        machine_jobs.sort(key=lambda job: job.processing_time + job.changeover_time)

    schedule = []
    current_time = 0
    for job in machine_jobs:
        start = current_time + job.changeover_time
        end = start + job.processing_time
        schedule.append((job.id, start, end))
        current_time = end
    return schedule, current_time

# Shifting Bottleneck Heuristic (SBH)
def shifting_bottleneck_heuristic(jobs, heuristic='EDD'):
    machine_jobs = defaultdict(list)
    for job in jobs:
        machine_jobs[job.machine_type].append(job)

    final_schedule = {}
    unscheduled_machines = list(machine_jobs.keys())

    while unscheduled_machines:
        machine_makespans = {}
        machine_schedules = {}

        for machine in unscheduled_machines:
            sched, makespan = schedule_machine(machine_jobs[machine], heuristic=heuristic)
            machine_makespans[machine] = makespan
            machine_schedules[machine] = sched

        bottleneck = max(machine_makespans, key=machine_makespans.get)
        final_schedule[bottleneck] = machine_schedules[bottleneck]
        unscheduled_machines.remove(bottleneck)

    return final_schedule

# Simulate disruptions (delays)
def apply_disruptions(schedule, disruption_rate=0.1):
    disrupted_schedule = {}
    for machine, jobs in schedule.items():
        new_jobs = []
        for job_id, start, end in jobs:
            if random.random() < disruption_rate:
                delay = random.randint(1, 3)
                print(f"⚠️ Disruption: Delaying {job_id} on {machine} by {delay}h")
                start += delay
                end += delay
            new_jobs.append((job_id, start, end))
        disrupted_schedule[machine] = new_jobs
    return disrupted_schedule

def apply_additional_disruptions(schedule, jobs, breakdown_rate=0.1, demand_rate=0.05, failure_rate=0.05, cancel_rate=0.05):
    disrupted_schedule = copy.deepcopy(schedule)
    job_ids = [job.id for job in jobs]

    # Machine breakdown: randomly delay all jobs on a machine
    for machine in disrupted_schedule:
        if random.random() < breakdown_rate:
            delay = random.randint(4, 12)
            print(f"⚠️ Machine breakdown on {machine}: All jobs delayed by {delay}h")
            disrupted_schedule[machine] = [
                (job_id, start + delay, end + delay) for job_id, start, end in disrupted_schedule[machine]
            ]

    # Unexpected demand: add a new job to a random machine
    if random.random() < demand_rate:
        machine = random.choice(list(disrupted_schedule.keys()))
        new_job_id = f"NEW{random.randint(100,999)}"
        last_end = max([end for _, _, end in disrupted_schedule[machine]], default=0)
        proc_time = random.randint(5, 15)
        changeover = random.choice([3, 24])
        start = last_end + changeover
        end = start + proc_time
        disrupted_schedule[machine].append((new_job_id, start, end))
        print(f"⚠️ Unexpected demand: Added job {new_job_id} to {machine}")

    # Product failure: randomly re-schedule a job (add rework)
    if random.random() < failure_rate:
        machine = random.choice(list(disrupted_schedule.keys()))
        if disrupted_schedule[machine]:
            failed_job = random.choice(disrupted_schedule[machine])
            rework_time = random.randint(3, 8)
            new_start = failed_job[2]
            new_end = new_start + rework_time
            disrupted_schedule[machine].append((failed_job[0] + "_REWORK", new_start, new_end))
            print(f"⚠️ Product failure: Rework for {failed_job[0]} on {machine}")

    # Order cancellation: randomly remove a job
    if random.random() < cancel_rate:
        machine = random.choice(list(disrupted_schedule.keys()))
        if disrupted_schedule[machine]:
            idx = random.randrange(len(disrupted_schedule[machine]))
            cancelled_job = disrupted_schedule[machine].pop(idx)
            print(f"⚠️ Order cancellation: Removed {cancelled_job[0]} from {machine}")

    return disrupted_schedule

# VNS Optimization Loop
def vns_optimization(jobs, iterations=100):
    best_jobs = copy.deepcopy(jobs)
    best_schedule = shifting_bottleneck_heuristic(best_jobs)
    best_makespan = calculate_total_makespan(best_schedule)

    for _ in range(iterations):
        new_jobs = perturb_jobs(copy.deepcopy(best_jobs))
        new_schedule = shifting_bottleneck_heuristic(new_jobs)
        new_makespan = calculate_total_makespan(new_schedule)

        if new_makespan < best_makespan:
            best_jobs = new_jobs
            best_schedule = new_schedule
            best_makespan = new_makespan

    return best_schedule

def calculate_total_makespan(schedule):
    return max(end for jobs in schedule.values() for _, _, end in jobs)

def perturb_jobs(jobs):
    new_jobs = jobs[:]
    machine_types = list(set(job.machine_type for job in new_jobs))
    target_machine = random.choice(machine_types)
    candidates = [j for j in new_jobs if j.machine_type == target_machine]
    if len(candidates) >= 2:
        a, b = random.sample(candidates, 2)
        a_idx = new_jobs.index(a)
        b_idx = new_jobs.index(b)
        new_jobs[a_idx], new_jobs[b_idx] = new_jobs[b_idx], new_jobs[a_idx]
    return new_jobs

# Machine group data
MACHINE_GROUPS = {
    '300 cm': {
        'num_machines': 12,
        'weekly_capacity': 500,
        'utilization': 0.95,
        'production_speed': 2.976  # metres per hour
    },
    '140 cm': {
        'num_machines': 16,
        'weekly_capacity': 900,
        'utilization': 0.62,
        'production_speed': 5.357
    },
    'Jacquard': {
        'num_machines': 3,
        'weekly_capacity': 750,
        'utilization': 0.53,
        'production_speed': 4.464
    }
}

def print_machine_group_info():
    print("===== Machine Group Information =====")
    print("Group      #Machines  WeeklyCap  Utilization  ProdSpeed(m/h)")
    for group, info in MACHINE_GROUPS.items():
        print(f"{group:10} {info['num_machines']:>9}  {info['weekly_capacity']:>9}  {info['utilization']*100:>9.1f}%  {info['production_speed']:>12.3f}")
    print()

# Print schedule
def print_schedule(schedule, title):
    print(f"\n===== {title} =====")
    for machine, sched in schedule.items():
        print(f"\nMachine: {machine}")
        for job_id, start, end in sched:
            print(f"  {job_id}: Start at {start}h, End at {end}h")

# Run all
if __name__ == "__main__":
    print_machine_group_info()
    random.seed(42)
    jobs = generate_jobs()

    initial_schedule = shifting_bottleneck_heuristic(jobs, heuristic='EDD')
    print_schedule(initial_schedule, "Initial Schedule (EDD)")

    disrupted = apply_disruptions(initial_schedule, disruption_rate=0.15)
    print_schedule(disrupted, "Disrupted Schedule")

    # Apply additional disruptions
    further_disrupted = apply_additional_disruptions(disrupted, jobs)
    print_schedule(further_disrupted, "Further Disrupted Schedule")

    optimized_schedule = vns_optimization(jobs, iterations=50)
    print_schedule(optimized_schedule, "Optimized Schedule (VNS)")

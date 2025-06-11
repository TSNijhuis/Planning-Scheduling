import random
import copy
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict

# Job structure
class Job:
    def __init__(self, job_id, machine_type, processing_time, changeover_time, due_date):
        self.id = job_id
        self.machine_type = machine_type
        self.processing_time = processing_time
        self.changeover_time = changeover_time
        self.due_date = due_date

# Machine group data
MACHINE_GROUPS = {
    '300 cm': {'num_machines': 12, 'weekly_capacity': 500, 'utilization': 0.95, 'production_speed': 2.976},
    '140 cm': {'num_machines': 16, 'weekly_capacity': 900, 'utilization': 0.62, 'production_speed': 5.357},
    'Jacquard': {'num_machines': 3, 'weekly_capacity': 750, 'utilization': 0.53, 'production_speed': 4.464}
}

# Generate jobs based on machine capacity distribution
def generate_jobs():
    total_jobs = 40
    mean_order_size = 400
    std_order_size = 40   #Can be adjusted accordingly by Kvadrat
    min_order_size = 200

    machine_types = ['140 cm', '300 cm', 'Jacquard']
    probs = [0.564, 0.360, 0.075]

    # Multinomial distribution for job counts per machine type
    job_counts = np.random.multinomial(total_jobs, probs)
    job_distribution = dict(zip(machine_types, job_counts))

    jobs = []
    job_count = 1

    for machine_type, num_jobs in job_distribution.items():
        prod_speed = MACHINE_GROUPS[machine_type]['production_speed']
        for _ in range(num_jobs):
            # Generate order size (meters), min 200
            order_size = max(int(np.random.normal(mean_order_size, std_order_size)), min_order_size)
            # Processing time in hours
            proc_time = int(order_size / prod_speed)
            changeover_time = 3 if random.random() < 0.8 else 24
            due_date = 168  
            jobs.append(Job(f"J{job_count}", machine_type, proc_time, changeover_time, due_date))
            job_count += 1

    print("Job distribution this run:", job_distribution)
    return jobs

# Assign jobs to individual machines in each group
def assign_jobs_to_individual_machines(jobs):
    machine_assignments = defaultdict(list)
    machine_types = set(job.machine_type for job in jobs)
    machine_indices = {mt: [f"{mt}_{i}" for i in range(MACHINE_GROUPS[mt]['num_machines'])] for mt in machine_types}

    # Sort jobs by due date (EDD)
    jobs = sorted(jobs, key=lambda job: job.due_date)

    for job in jobs:
        group = job.machine_type
        best_machine = None
        earliest_start = float('inf')
        for machine in machine_indices[group]:
            # Simulate adding job to this machine and get its start time
            temp_jobs = machine_assignments[machine] + [job]
            schedule = schedule_machine(temp_jobs)
            for job_id, start, end in schedule:
                if job_id == job.id and start < earliest_start:
                    # Check capacity constraint
                    total_proc = sum(j.processing_time for j in machine_assignments[machine]) + job.processing_time
                    if total_proc <= MACHINE_GROUPS[group]['weekly_capacity']:
                        earliest_start = start
                        best_machine = machine
        if best_machine:
            machine_assignments[best_machine].append(job)
        else:
            print(f"⚠️ Warning: Could not assign job {job.id} to any {group} machine without exceeding weekly capacity.")

    return machine_assignments

# Schedule jobs on a single machine using EDD
def schedule_machine(machine_jobs):
    machine_jobs.sort(key=lambda job: job.due_date)
    schedule = []
    current_time = 0
    for idx, job in enumerate(machine_jobs):
        if idx == 0:
            start = 0
        else:
            start = current_time + job.changeover_time
        end = start + job.processing_time
        schedule.append((job.id, start, end))
        current_time = end
    return schedule

# Shifting Bottleneck Heuristic with parallel machines
def shifting_bottleneck_parallel(jobs):
    machine_assignments = assign_jobs_to_individual_machines(jobs)
    all_machines = list(machine_assignments.keys())
    improved = True

    while improved:
        improved = False
        lateness_dict = {}
        temp_schedules = {}

        # Schedule each machine and calculate its max lateness
        for machine in all_machines:
            schedule = schedule_machine(machine_assignments[machine])
            temp_schedules[machine] = schedule
            max_lateness = 0
            for job_id, start, end in schedule:
                job = next(j for j in machine_assignments[machine] if j.id == job_id)
                lateness = max(0, end - job.due_date)
                if lateness > max_lateness:
                    max_lateness = lateness
            lateness_dict[machine] = max_lateness

        # Find the bottleneck machine (with highest max lateness)
        bottleneck_machine = max(lateness_dict, key=lateness_dict.get)
        bottleneck_jobs = machine_assignments[bottleneck_machine][:]
        group = bottleneck_machine.split('_')[0]
        group_machines = [m for m in all_machines if m.startswith(group)]

        # Try to move each job to any earlier slot on any machine of the same type
        for job in bottleneck_jobs:
            current_schedule = schedule_machine(machine_assignments[bottleneck_machine])
            current_start = None
            for job_id, start, end in current_schedule:
                if job_id == job.id:
                    current_start = start
                    break

            for other_machine in group_machines:
                if other_machine == bottleneck_machine:
                    continue
                # Try inserting job at every possible position
                for insert_pos in range(len(machine_assignments[other_machine]) + 1):
                    # Remove from current machine
                    machine_assignments[bottleneck_machine].remove(job)
                    # Insert into new machine at position insert_pos
                    machine_assignments[other_machine].insert(insert_pos, job)

                    # Recalculate schedule for other_machine
                    new_schedule = schedule_machine(machine_assignments[other_machine])
                    new_start = None
                    for job_id, start, end in new_schedule:
                        if job_id == job.id:
                            new_start = start
                            break

                    # Check capacity constraint
                    total_proc = sum(j.processing_time for j in machine_assignments[other_machine])
                    if (new_start is not None and current_start is not None and
                        new_start < current_start and
                        total_proc <= MACHINE_GROUPS[group]['weekly_capacity']):
                        improved = True
                        break
                    else:
                        # Undo the move
                        machine_assignments[other_machine].pop(insert_pos)
                        machine_assignments[bottleneck_machine].append(job)
                if improved:
                    break
            if improved:
                break  # Only one move per iteration

    # Final scheduling after all improvements
    final_schedule = {}
    for machine in all_machines:
        final_schedule[machine] = schedule_machine(machine_assignments[machine])
    return final_schedule

# Apply additional disruptions
def apply_additional_disruptions(jobs,  demand_rate=0.05):


    if random.random() < demand_rate:
        machine_types = ['140 cm', '300 cm', 'Jacquard']
        probs = [0.564, 0.360, 0.075]
        # Randomly select machine type for the new job based on probabilities
        machine_type = np.random.choice(machine_types, p=probs)
        prod_speed = MACHINE_GROUPS[machine_type]['production_speed']
        mean_order_size = 400
        std_order_size = 40   # Can be adjusted accordingly by Kvadrat
        min_order_size = 200
        order_size = max(int(np.random.normal(mean_order_size, std_order_size)), min_order_size)
        proc_time = int(order_size / prod_speed)
        changeover_time = 3 if random.random() < 0.8 else 24
        due_date = int(proc_time + random.gauss(6, 2)) # Can be adjusted accordingly by Kvadrat
        new_job_id = f"NEW{random.randint(100,999)}"
        # Add the new job to the jobs list
        jobs.append(Job(new_job_id, machine_type, proc_time, changeover_time, due_date))
        print(f"⚠️ Unexpected demand: Added job {new_job_id} ({machine_type}, {order_size}m, {proc_time}h) to jobs list")

    return jobs

# VNS Optimization
def vns_optimization(jobs, iterations=100):
    best_jobs = copy.deepcopy(jobs)
    best_schedule = shifting_bottleneck_parallel(best_jobs)
    best_makespan = calculate_total_makespan(best_schedule)

    for _ in range(iterations):
        new_jobs = perturb_jobs(copy.deepcopy(best_jobs))
        new_schedule = shifting_bottleneck_parallel(new_jobs)
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

def print_machine_group_info():
    print("===== Machine Group Information =====")
    print("Group      #Machines  WeeklyCap  Utilization  ProdSpeed(m/h)")
    for group, info in MACHINE_GROUPS.items():
        print(f"{group:10} {info['num_machines']:>9}  {info['weekly_capacity']:>9}  {info['utilization']*100:>9.1f}%  {info['production_speed']:>12.3f}")
    print()

def print_schedule(schedule, title):
    print(f"\n===== {title} =====")
    for machine, sched in sorted(schedule.items()):
        print(f"\nMachine: {machine}")
        for job_id, start, end in sched:
            print(f"  {job_id}: Start at {start}h, End at {end}h")

# Gantt chart visualization
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import random

def plot_gantt_chart(schedule, title="Final Job Shop Schedule", save_path="final_schedule.png"):
    fig, ax = plt.subplots(figsize=(12, 8))
    colors = {}

    machine_names = list(schedule.keys())
    machine_names.sort()
    yticks = []
    yticklabels = []

    for i, machine in enumerate(machine_names):
        jobs = schedule[machine]
        yticks.append(i)
        yticklabels.append(machine)
        for job_id, start, end in jobs:
            if job_id not in colors:
                colors[job_id] = (random.random(), random.random(), random.random())
            ax.barh(i, end - start, left=start, height=0.4, color=colors[job_id])
            ax.text(start + (end - start) / 2, i, job_id, va='center', ha='center', color='white', fontsize=8)

    ax.set_yticks(yticks)
    ax.set_yticklabels(yticklabels)
    ax.set_xlabel("Time (hours)")
    ax.set_title(title)
    ax.grid(True)

    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()

def run_full_scheduling_pipeline(jobs, vns_iterations=50):
    # 1. Assign jobs to machines and schedule using EDD
    machine_assignments = assign_jobs_to_individual_machines(jobs)
    initial_schedule = {}
    for machine, job_list in machine_assignments.items():
        initial_schedule[machine] = schedule_machine(job_list)
    print_schedule(initial_schedule, "Initial EDD Schedule (per machine)")

    # 2. Shifting Bottleneck Heuristic (SBH)
    sbh_schedule = shifting_bottleneck_parallel(jobs)
    print_schedule(sbh_schedule, "Shifting Bottleneck Heuristic Schedule")

    # 3. VNS Optimization
    optimized_schedule = vns_optimization(jobs, iterations=vns_iterations)
    print_schedule(optimized_schedule, "Optimized Schedule (VNS)")

    return initial_schedule, sbh_schedule, optimized_schedule

# Run all
if __name__ == "__main__":
    print_machine_group_info()
    random.seed(42)
    jobs = generate_jobs()

    # 1. Print initial schedule
    print("\n--- Initial Schedule ---")
    initial_schedule, _, _ = run_full_scheduling_pipeline(jobs, vns_iterations=0)

    # 2. Apply disruptions (adds new job to jobs list)
    print("\n--- Applying Disruption (Unexpected Demand) ---")
    apply_additional_disruptions(jobs, demand_rate=0.05)  # Set demand_rate=1.0 to guarantee a new job is added

    # 3. Print new schedule with the disruption
    print("\n--- Fixed Schedule After Disruption ---")
    fixed_schedule, _, _ = run_full_scheduling_pipeline(jobs, vns_iterations=0)

    # 4. Gantt chart for the optimized schedule
    plot_gantt_chart(fixed_schedule, title="Fixed Schedule After Disruption")

    # 4. Gantt chart for the optimized schedule
    plot_gantt_chart(fixed_schedule, title="Fixed Schedule After Disruption")


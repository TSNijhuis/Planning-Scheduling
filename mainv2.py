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
        self.status = 'not_started'
        self.remaining_time = processing_time
        self.start_time = None
        self.end_time = None

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
    
    std_order_size = 40 #Can be adjusted accordingly by Kvadrat
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
    if not jobs:
        return defaultdict(list)
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
            print(f"Warning: Could not assign job {job.id} to any {group} machine without exceeding weekly capacity.")

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
    if not jobs:
        return {}, {}
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

        # Find the current global max lateness
        global_max_lateness = max(lateness_dict.values())

        # Try to move any job to any position on any machine of the same type
        for machine in all_machines:
            group = machine.split('_')[0]
            group_machines = [m for m in all_machines if m.startswith(group)]
            for job in machine_assignments[machine][:]:
                for target_machine in group_machines:
                    for insert_pos in range(len(machine_assignments[target_machine]) + 1):
                        if target_machine == machine and insert_pos > machine_assignments[machine].index(job):
                            continue # Don't re-insert at the same or later position in the same machine

                        # Remove from current machine
                        machine_assignments[machine].remove(job)
                        # Insert into target machine at position insert_pos
                        machine_assignments[target_machine].insert(insert_pos, job)

                        # Recalculate all schedules and lateness
                        new_schedules = {m: schedule_machine(machine_assignments[m]) for m in all_machines}
                        new_lateness = 0
                        for m, sched in new_schedules.items():
                            for job_id, start, end in sched:
                                job_obj = next(j for j in machine_assignments[m] if j.id == job_id)
                                lateness = max(0, end - job_obj.due_date)
                                if lateness > new_lateness:
                                    new_lateness = lateness

                        # Accept the move if it improves global max lateness
                        if new_lateness < global_max_lateness:
                            improved = True
                            break
                        else:
                            # Undo the move
                            machine_assignments[target_machine].pop(insert_pos)
                            machine_assignments[machine].insert(
                                min(insert_pos, len(machine_assignments[machine])), job)
                    if improved:
                        break
                if improved:
                    break
            if improved:
                break

    # Final scheduling after all improvements
    final_schedule = {}
    for machine in all_machines:
        final_schedule[machine] = schedule_machine(machine_assignments[machine])
    return final_schedule, machine_assignments

# Apply additional disruptions
def apply_additional_disruptions(jobs, demand_rate=1, cancel_rate=1, breakdown_rate=1, machine_assignments=None, reschedulable_jobs=None):
    if reschedulable_jobs is None:
        reschedulable_jobs = []
    # --- Unexpected Demand ---
    if random.random() < demand_rate:
        machine_types = ['140 cm', '300 cm', 'Jacquard']
        probs = [0.564, 0.360, 0.076]
        machine_type = np.random.choice(machine_types, p=probs)
        prod_speed = MACHINE_GROUPS[machine_type]['production_speed']
        mean_order_size = 400
        std_order_size = 40
        min_order_size = 200
        order_size = max(int(np.random.normal(mean_order_size, std_order_size)), min_order_size)
        proc_time = int(order_size / prod_speed)
        changeover_time = 3 if random.random() < 0.8 else 24
        due_date = 168
        new_job_id = f"NEW{random.randint(100,999)}"
        jobs.append(Job(new_job_id, machine_type, proc_time, changeover_time, due_date))
        print(f"Unexpected demand: Added job {new_job_id} ({machine_type}, {order_size}m, {proc_time}h) to jobs list")

    # --- Job Cancellation ---
    if jobs and random.random() < cancel_rate:
        cancel_job = random.choice(jobs)
        jobs.remove(cancel_job)
        print(f"Job cancellation: Removed job {cancel_job.id} ({cancel_job.machine_type}) from jobs list")

    # --- Machine Breakdown ---
    if machine_assignments and random.random() < breakdown_rate:
        machine_types = ['140 cm', '300 cm', 'Jacquard']
        breakdown_type = random.choice(machine_types)
        machines_of_type = [m for m in machine_assignments if m.startswith(breakdown_type)]
        if machines_of_type:
            broken_machine = random.choice(machines_of_type)
            queue = machine_assignments[broken_machine]['queue'] if isinstance(machine_assignments[broken_machine], dict) else machine_assignments[broken_machine]
            broken_jobs = [job for job in queue if job.status != 'finished']
            print(f"Machine breakdown: {broken_machine} is down. Marking {len(broken_jobs)} jobs for rescheduling.")
            # Remove jobs from the broken machine's queue
            machine_assignments[broken_machine]['queue'] = [job for job in queue if job.status == 'finished']
            # Set status to 'not_started' and clear start/end times for jobs to be rescheduled
            for job in broken_jobs:
                job.status = 'not_started'
                job.start_time = None
                job.end_time = None
            # Remove the broken machine from the assignments so it won't be used again
            del machine_assignments[broken_machine]
    elif random.random() < breakdown_rate:
        print("Warning: Machine breakdown requested but no machine_assignments provided.")

    return jobs, reschedulable_jobs

def vns_optimization(jobs, iterations=100):
    best_jobs = copy.deepcopy(jobs)
    best_schedule, best_assignments = shifting_bottleneck_parallel(best_jobs)
    best_makespan = calculate_total_makespan(best_schedule)

    for _ in range(iterations):
        new_jobs = perturb_jobs(copy.deepcopy(best_jobs))
        new_schedule, new_assignments = shifting_bottleneck_parallel(new_jobs)
        new_makespan = calculate_total_makespan(new_schedule)

        if new_makespan < best_makespan:
            best_jobs = new_jobs
            best_schedule = new_schedule
            best_assignments = new_assignments
            best_makespan = new_makespan

    return best_schedule, best_assignments

def calculate_total_makespan(schedule):
    return max((end for jobs in schedule.values() for _, _, end in jobs), default=0)

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
    print("Group #Machines WeeklyCap Utilization ProdSpeed(m/h)")
    for group, info in MACHINE_GROUPS.items():
        print(f"{group:10} {info['num_machines']:>9} {info['weekly_capacity']:>9} {info['utilization']*100:>9.1f}% {info['production_speed']:>12.3f}")
    print()

def print_schedule(schedule, title):
    print(f"\n===== {title} =====")
    for machine, sched in sorted(schedule.items()):
        print(f"\nMachine: {machine}")
        for job_id, start, end in sched:
            print(f" {job_id}: Start at {start}h, End at {end}h")

# Gantt chart visualization
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import random

def plot_gantt_chart(schedule, title="Final Job Shop Schedule", save_path="final_schedule.png", max_lateness=None):
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

    # Annotate max lateness at the bottom right
    if max_lateness is not None:
        ax.text(
            0.99, 0.01,
            f"Max lateness: {max_lateness}",
            transform=ax.transAxes,
            fontsize=12,
            color='red',
            ha='right',
            va='bottom',
            bbox=dict(facecolor='white', alpha=0.7, edgecolor='red')
        )

    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()

def calculate_max_lateness(schedule, jobs):
    job_dict = {job.id: job for job in jobs}
    max_lateness = 0
    for machine_jobs in schedule.values():
        for job_id, start, end in machine_jobs:
            due_date = job_dict[job_id].due_date
            lateness = max(0, end - due_date)
            if lateness > max_lateness:
                max_lateness = lateness
    return max_lateness

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

    # 3. VNS Optimization (run on SBH result)
    optimized_schedule = vns_optimization(jobs, iterations=vns_iterations)
    print_schedule(optimized_schedule, "Optimized Schedule (VNS)")

    # Return all three for flexibility, but use optimized_schedule as the final
    return initial_schedule, sbh_schedule, optimized_schedule

# Run all
if __name__ == "__main__":
    random.seed(42)
    jobs = generate_jobs()

    # 1. Initial EDD schedule
    machine_assignments = assign_jobs_to_individual_machines(jobs)
    initial_edd_schedule = {}
    for machine, job_list in machine_assignments.items():
        initial_edd_schedule[machine] = schedule_machine(job_list)
    max_late = calculate_max_lateness(initial_edd_schedule, jobs)
    plot_gantt_chart(initial_edd_schedule, title="Initial EDD Schedule", save_path="initial_edd_schedule.png", max_lateness=max_late)

    # 2. After SBH
    sbh_schedule = shifting_bottleneck_parallel(jobs)
    max_late_sbh = calculate_max_lateness(sbh_schedule, jobs)
    plot_gantt_chart(sbh_schedule, title="After Shifting Bottleneck Heuristic (SBH)", save_path="after_sbh.png", max_lateness=max_late_sbh)

    # 3. After VNS
    vns_schedule = vns_optimization(jobs, iterations=50)
    max_late_vns = calculate_max_lateness(vns_schedule, jobs)
    plot_gantt_chart(vns_schedule, title="After VNS Optimization", save_path="after_vns.png", max_lateness=max_late_vns)

    # 4. Apply disruptions (adds new job to jobs list)
    print("\n--- Applying Disruptions ---")
    apply_additional_disruptions(jobs, demand_rate=0.05)

    # 5. Initial EDD schedule after disruption
    machine_assignments_disrupted = assign_jobs_to_individual_machines(jobs)
    initial_edd_schedule_disrupted = {}
    for machine, job_list in machine_assignments_disrupted.items():
        initial_edd_schedule_disrupted[machine] = schedule_machine(job_list)
    max_late_disrupted = calculate_max_lateness(initial_edd_schedule_disrupted, jobs)
    plot_gantt_chart(initial_edd_schedule_disrupted, title="Initial EDD Schedule After Disruption", save_path="initial_edd_schedule_after_disruption.png", max_lateness=max_late_disrupted)

    # 6. After SBH (post-disruption)
    sbh_schedule_disrupted = shifting_bottleneck_parallel(jobs)
    max_late_sbh_disrupted = calculate_max_lateness(sbh_schedule_disrupted, jobs)
    plot_gantt_chart(sbh_schedule_disrupted, title="After SBH (Post-Disruption)", save_path="after_sbh_post_disruption.png", max_lateness=max_late_sbh_disrupted)

    # 7. After VNS (post-disruption)
    vns_schedule_disrupted = vns_optimization(jobs, iterations=50)
    max_late_vns_disrupted = calculate_max_lateness(vns_schedule_disrupted, jobs)
    plot_gantt_chart(vns_schedule_disrupted, title="After VNS (Post-Disruption)", save_path="after_vns_post_disruption.png", max_lateness=max_late_vns_disrupted)

import random
import copy
from mainv2 import calculate_max_lateness, generate_jobs, assign_jobs_to_individual_machines, apply_additional_disruptions, schedule_machine, shifting_bottleneck_parallel, vns_optimization, plot_gantt_chart, MACHINE_GROUPS

def simulate_over_time(total_hours=168, disruption_rates=(0.05, 0.05, 0.05)):
    # disruption_rates: (demand, cancel, breakdown)
    demand_rate, cancel_rate, breakdown_rate = disruption_rates
    no_disruption_rate = 1 - sum(disruption_rates)
    disruption_choices = ['demand', 'cancel', 'breakdown', 'none']
    disruption_probs = [demand_rate, cancel_rate, breakdown_rate, no_disruption_rate]
    # Initial assignment
    jobs = generate_jobs()
    machine_assignments = assign_jobs_to_individual_machines(jobs)
    # For each machine, keep a queue of jobs (in order) and track current job
    machine_states = {}
    for machine, job_list in machine_assignments.items():
        machine_states[machine] = {
            'queue': list(job_list),  # jobs assigned to this machine
            'current_job': None,
            'remaining_time': 0,
            'job_idx': 0  # index in queue
        }

    # Track job status
    for job in jobs:
        job.status = 'not_started'
        job.remaining_time = job.processing_time
        job.start_time = None
        job.end_time = None

    # --- INITIAL SCHEDULE AND PLOT ---
    initial_schedule = {}
    for machine, job_list in machine_assignments.items():
        initial_schedule[machine] = schedule_machine(job_list)

    # Plot the initial schedule
    max_late = calculate_max_lateness(initial_schedule, jobs)
    """plot_gantt_chart(
        initial_schedule,
        title="Initial Gantt Chart before simulation",
        save_path="gantt_initial.png",
        max_lateness=max_late
    )"""

    for t in range(total_hours):
        print(f"\n=== Time step {t} ===")
        # 1. Update job progress on each machine
        for machine, state in machine_states.items():
            # If a job is running, decrement its remaining time
            job = state['current_job']
            # 1. If a job is running, decrement its remaining time
            if job:
                job.remaining_time -= 1
                if job.remaining_time <= 0:
                    job.status = 'finished'
                    job.end_time = t
                    print(f"Job {job.id} finished on {machine} at time {t}")
                    state['current_job'] = None
                    state['remaining_time'] = 0
                    # Prepare for changeover before next job
                    # Only set changeover if there is another job to do
                    if state['job_idx'] < len(state['queue']):
                        next_job = state['queue'][state['job_idx']]
                        state['changeover_remaining'] = next_job.changeover_time
                    else:
                        state['changeover_remaining'] = 0

            # 2. If idle, handle changeover or start next job
            if not state['current_job']:
                # Handle changeover period
                if state.get('changeover_remaining', 0) > 0:
                    state['changeover_remaining'] -= 1
                elif state['job_idx'] < len(state['queue']):
                    next_job = state['queue'][state['job_idx']]
                    if next_job.status == 'not_started':
                        next_job.status = 'in_progress'
                        next_job.start_time = t
                        state['current_job'] = next_job
                        state['remaining_time'] = next_job.remaining_time
                        print(f"Job {next_job.id} started on {machine} at time {t}")
                        state['job_idx'] += 1

        # 2. Decide which disruption (if any) occurs
        disruption_type = random.choices(disruption_choices, weights=disruption_probs, k=1)[0]
        disruption_occurred = False

        if disruption_type == 'demand':
            jobs, _ = apply_additional_disruptions(jobs, demand_rate=1, cancel_rate=0, breakdown_rate=0, machine_assignments=machine_states, reschedulable_jobs=[])
            disruption_occurred = True
        elif disruption_type == 'cancel':
            jobs, _ = apply_additional_disruptions(jobs, demand_rate=0, cancel_rate=1, breakdown_rate=0, machine_assignments=machine_states, reschedulable_jobs=[])
            disruption_occurred = True
        elif disruption_type == 'breakdown':
            jobs, _ = apply_additional_disruptions(
                jobs, demand_rate=0, cancel_rate=0, breakdown_rate=1,
                machine_assignments=machine_states, reschedulable_jobs=[]
            )
        # 3. If disruption occurred, reschedule not_started jobs
        if disruption_occurred:
            print(f"Disruption at t={t}, rescheduling not started jobs.")
            print(f"Total jobs at t={t}: {len(jobs)}")
            print(f"Not started jobs at t={t}: {[j.id for j in jobs if j.status == 'not_started']}")
            print(f"In progress jobs at t={t}: {[j.id for j in jobs if j.status == 'in_progress']}")
            print(f"Finished jobs at t={t}: {[j.id for j in jobs if j.status == 'finished']}")
            unfinished_jobs = [j for j in jobs if j.status == 'not_started']

            # After rescheduling:
            final_schedule, new_assignments = vns_optimization(unfinished_jobs, iterations=100)

            # Update machine queues for not_started jobs only
            for machine in machine_states:
                # Keep jobs that are already started or finished
                in_progress_or_done = [j for j in machine_states[machine]['queue'] if j.status != 'not_started']
                # Only add new assignments for not_started jobs
                machine_states[machine]['queue'] = in_progress_or_done + new_assignments.get(machine, [])
                state = machine_states[machine]
                state['job_idx'] = len(in_progress_or_done)

            # Build a combined schedule for plotting
            combined_schedule = {}
            job_ids_set = {job.id for job in jobs}
            for machine in machine_states:
                # 1. Jobs already started or finished (with actual times)
                actual_jobs = [
                    (j.id, j.start_time, j.end_time if j.status == 'finished' else (j.start_time + j.processing_time))
                    for j in machine_states[machine]['queue']
                    if j.status != 'not_started' and j.start_time is not None and j.id in job_ids_set
                ]
                # Find the latest end time on this machine
                # Find the last actual job (finished or in progress) and its changeover time
                if actual_jobs:
                    last_actual_job_id, _, last_actual_end = actual_jobs[-1]
                    last_job_obj = next((j for j in jobs if j.id == last_actual_job_id), None)
                    last_end = last_actual_end + (last_job_obj.changeover_time if last_job_obj else 0)
                else:
                    last_end = 0

                planned_jobs = []
                for tup in final_schedule.get(machine, []):
                    job_id, start, end = tup
                    if job_id in job_ids_set and not any(j[0] == job_id for j in actual_jobs):
                        job_obj = next((j for j in jobs if j.id == job_id), None)
                        changeover = job_obj.changeover_time if job_obj else 0
                        # Always add changeover after the previous job (actual or planned), except before the very first job
                        if actual_jobs or planned_jobs:
                            last_end += changeover
                        planned_start = max(start, last_end)
                        planned_end = planned_start + (end - start)
                        planned_jobs.append((job_id, planned_start, planned_end))
                        last_end = planned_end
                combined_schedule[machine] = actual_jobs + planned_jobs

            max_late = calculate_max_lateness(combined_schedule, jobs)
            """plot_gantt_chart(
                combined_schedule,
                title=f"Gantt Chart after disruption at t={t}",
                save_path=f"gantt_after_disruption_t{t}.png",
                max_lateness=max_late
            )"""
            
    # Print summary
    print("\n=== Simulation finished ===")
    for job in jobs:
        print(f"{job.id}: {job.status}, start={job.start_time}, end={job.end_time}")

    return max_late

if __name__ == "__main__":
    results = []
    lateness_values = []  # Store all max_lateness values
    Max_Max_lateness = float('-inf')

    for week in range(1, 53):  # 52 unique seeds, one for each week
        print(f"\n=== Experiment (Week) {week} ===")
        random.seed(42 + week)  # Set unique, reproducible seed for each run
        max_lateness = simulate_over_time(
            total_hours=168,
            disruption_rates=(0.017, 0.0089, 0.0013)
        )
        lateness_values.append(max_lateness)
        Max_Max_lateness = max(Max_Max_lateness, max_lateness)

    Average_max_lateness = sum(lateness_values) / len(lateness_values)

    print("\n=== All 52 Weekly Experiments Finished ===")
    print("Max lateness per week:", lateness_values)
    print(f"Average max lateness: {Average_max_lateness}")
    print(f"Maximum of max lateness: {Max_Max_lateness}")

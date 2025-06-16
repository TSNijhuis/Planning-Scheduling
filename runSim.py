import random
import copy
from mainv2 import calculate_max_lateness, generate_jobs, assign_jobs_to_individual_machines, apply_additional_disruptions, schedule_machine, shifting_bottleneck_parallel, vns_optimization, plot_gantt_chart, MACHINE_GROUPS

def extract_jobs(job_list):
    """Ensure a flat list of Job objects, even if input is a list of tuples or strings."""
    jobs = []
    for item in job_list:
        if hasattr(item, 'machine_type'):
            jobs.append(item)
        elif isinstance(item, tuple) and hasattr(item[0], 'machine_type'):
            jobs.append(item[0])
    return jobs

def simulate_over_time(jobs, total_hours=168, disruption_rates=(0.05, 0.05, 0.05)):
    # disruption_rates: (demand, cancel, breakdown)
    demand_rate, cancel_rate, breakdown_rate = disruption_rates
    no_disruption_rate = 1 - sum(disruption_rates)
    disruption_choices = ['demand', 'cancel', 'breakdown', 'none']
    disruption_probs = [demand_rate, cancel_rate, breakdown_rate, no_disruption_rate]
    # Initial assignment
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

    for t in range(total_hours):
        print(f"\n=== Time step {t} ===")
        # 1. Update job progress on each machine
        for machine, state in machine_states.items():
            # If a job is running, decrement its remaining time
            job = state['current_job']
            if job:
                job.remaining_time -= 1
                if job.remaining_time <= 0:
                    job.status = 'finished'
                    job.end_time = t
                    print(f"Job {job.id} finished on {machine} at time {t}")
                    state['current_job'] = None
                    state['remaining_time'] = 0

            # If idle, start next job in queue
            if not state['current_job']:
                while state['job_idx'] < len(state['queue']):
                    next_job = state['queue'][state['job_idx']]
                    if next_job.status == 'not_started':
                        next_job.status = 'in_progress'
                        next_job.start_time = t
                        state['current_job'] = next_job
                        state['remaining_time'] = next_job.remaining_time
                        print(f"Job {next_job.id} started on {machine} at time {t}")
                        state['job_idx'] += 1
                        break
                    state['job_idx'] += 1

        # 2. Decide which disruption (if any) occurs
        disruption_type = random.choices(disruption_choices, weights=disruption_probs, k=1)[0]
        disruption_occurred = False

        if disruption_type == 'demand':
            jobs = apply_additional_disruptions(jobs, demand_rate=1, cancel_rate=0, breakdown_rate=0, machine_assignments=machine_states)
            disruption_occurred = True
        elif disruption_type == 'cancel':
            jobs = apply_additional_disruptions(jobs, demand_rate=0, cancel_rate=1, breakdown_rate=0, machine_assignments=machine_states)
            disruption_occurred = True
        elif disruption_type == 'breakdown':
            jobs = apply_additional_disruptions(jobs, demand_rate=0, cancel_rate=0, breakdown_rate=1,machine_assignments=machine_states)
            disruption_occurred = True

        # 3. If disruption occurred, reschedule not_started jobs
        if disruption_occurred:
            print(f"Disruption at t={t}, rescheduling not started jobs.")
            print(f"Total jobs at t={t}: {len(jobs)}")
            print(f"Not started jobs at t={t}: {[j.id for j in jobs if j.status == 'not_started']}")
            print(f"In progress jobs at t={t}: {[j.id for j in jobs if j.status == 'in_progress']}")
            print(f"Finished jobs at t={t}: {[j.id for j in jobs if j.status == 'finished']}")
            unfinished_jobs = [j for j in jobs if j.status == 'not_started']

            # --- Use shifting bottleneck heuristic and VNS for rescheduling ---
            new_assignments = vns_optimization(unfinished_jobs, iterations=100)  

            # Update machine queues for not_started jobs only
            for machine in machine_states:
                in_progress = [j for j in machine_states[machine]['queue'] if j.status != 'not_started']
                machine_states[machine]['queue'] = in_progress + new_assignments.get(machine, [])
                state = machine_states[machine]
                state['job_idx'] = len(in_progress)
            
            # Build the schedule for plotting
            final_schedule = {}
            for machine, state in machine_states.items():
                jobs_to_plot = [j for j in state['queue'] if j.start_time is not None and j.end_time is not None]
                final_schedule[machine] = [(j.id, j.start_time, j.end_time) for j in jobs_to_plot]

            # Calculate max lateness for the current jobs
            max_late = calculate_max_lateness(final_schedule, jobs)

            # Call plot_gantt_chart with all arguments
            plot_gantt_chart(
                final_schedule,
                title=f"Gantt Chart after disruption at t={t}",
                save_path=f"gantt_after_disruption_t{t}.png",
                max_lateness=max_late
            )
            
    # Print summary
    print("\n=== Simulation finished ===")
    for job in jobs:
        print(f"{job.id}: {job.status}, start={job.start_time}, end={job.end_time}")

if __name__ == "__main__":
    random.seed(42)
    jobs = generate_jobs()
    simulate_over_time(jobs, total_hours=168, disruption_rates=(0.05, 0.05, 0.05))
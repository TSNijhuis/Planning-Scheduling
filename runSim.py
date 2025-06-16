import random
import os
import sys
import copy
from mainv2 import calculate_max_lateness, generate_jobs, assign_jobs_to_individual_machines, apply_additional_disruptions, schedule_machine, shifting_bottleneck_parallel, vns_optimization, plot_gantt_chart, MACHINE_GROUPS
import tkinter as tk
from tkinter import messagebox, ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image, ImageTk

def simulate_over_time(self,total_hours=168, disruption_rates=(0.05, 0.05, 0.05)):
    # disruption_rates: (demand, cancel, breakdown)
    demand_rate = float(self.demand_entry.get())
    cancel_rate = float(self.cancel_entry.get())
    breakdown_rate = float(self.breakdown_entry.get())
    
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
            'queue': list(job_list), # jobs assigned to this machine
            'current_job': None,
            'remaining_time': 0,
            'job_idx': 0 # index in queue
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
    # plot_gantt_chart(
    #     initial_schedule,
    #     title="Initial Gantt Chart before simulation",
    #     save_path="gantt_initial.png",
    #     max_lateness=max_late
    # )

    for t in range(total_hours):
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
            jobs, _ = apply_additional_disruptions(jobs, demand_rate=0, cancel_rate=0, breakdown_rate=1, machine_assignments=machine_states, reschedulable_jobs=[])
            disruption_occurred = True

        # 3. If disruption occurred, reschedule not_started jobs
        if disruption_occurred:
            unfinished_jobs = [j for j in jobs if j.status == 'not_started']
            final_schedule, new_assignments = vns_optimization(unfinished_jobs, iterations=100)

            # Update machine queues for not_started jobs only
            for machine in machine_states:
                in_progress_or_done = [j for j in machine_states[machine]['queue'] if j.status != 'not_started']
                machine_states[machine]['queue'] = in_progress_or_done 
                if machine in new_assignments:
                    machine_states[machine]['queue'].extend(new_assignments[machine])
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
            # plot_gantt_chart(
            #     combined_schedule,
            #     title=f"Gantt Chart after disruption at t={t}",
            #     save_path=f"gantt_after_disruption_t{t}.png",
            #     max_lateness=max_late
            # )
            return calculate_max_lateness(combined_schedule, jobs)

class SimulationGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Planning-Scheduling Simulation")
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        
        # Setup GUI components
        self.setup_ui()
        
    def setup_ui(self):
        # Logo frame
        logo_frame = tk.Frame(self.root, bg="#f5f5f5")
        logo_frame.pack(fill=tk.X, pady=(10, 0))
        try:
            logo_img = Image.open("kvadrat-seeklogo.png")
            logo_img = logo_img.resize((120, 40), Image.LANCZOS)
            self.logo_photo = ImageTk.PhotoImage(logo_img)
            logo_label = tk.Label(logo_frame, image=self.logo_photo, bg="#f5f5f5")
            logo_label.pack(pady=5)
        except Exception as e:
            logo_label = tk.Label(logo_frame, text="Kvadrat", font=("Arial", 24, "bold"), bg="#f5f5f5", fg="#333")
            logo_label.pack(pady=5)
        # Input frame
        input_frame = tk.Frame(self.root)
        input_frame.pack(pady=10)
        
        tk.Label(input_frame, text="Number of Experiments:").pack(side=tk.LEFT)
        self.entry = tk.Entry(input_frame, width=10)
        self.entry.insert(0, "52")
        self.entry.pack(side=tk.LEFT, padx=5)

        tk.Label(input_frame, text="Demand Rate:").pack(side=tk.LEFT)
        self.demand_entry = tk.Entry(input_frame, width=5)
        self.demand_entry.insert(0, "0.017")
        self.demand_entry.pack(side=tk.LEFT, padx=2)

        tk.Label(input_frame, text="Cancel Rate:").pack(side=tk.LEFT)
        self.cancel_entry = tk.Entry(input_frame, width=5)
        self.cancel_entry.insert(0, "0.0089")
        self.cancel_entry.pack(side=tk.LEFT, padx=2)

        tk.Label(input_frame, text="Breakdown Rate:").pack(side=tk.LEFT)
        self.breakdown_entry = tk.Entry(input_frame, width=5)
        self.breakdown_entry.insert(0, "0.0013")
        self.breakdown_entry.pack(side=tk.LEFT, padx=2)

        
        self.run_button = tk.Button(input_frame, text="Run Simulation", command=self.start_simulation)
        self.run_button.pack(side=tk.LEFT, padx=5)
        
        # Progress bar
        self.progress_frame = tk.Frame(self.root)
        self.progress_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.progress_label = tk.Label(self.progress_frame, text="Ready")
        self.progress_label.pack(side=tk.LEFT)
        
        self.progress_bar = ttk.Progressbar(self.progress_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.progress_bar.pack(fill=tk.X, expand=True)
        
        # Results frame
        results_frame = tk.Frame(self.root)
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Matplotlib figure
        self.fig, self.ax = plt.subplots(figsize=(8, 4))
        self.canvas = FigureCanvasTkAgg(self.fig, master=results_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # Summary label
        self.summary_label = tk.Label(self.root, text="", font=('Arial', 10))
        self.summary_label.pack(pady=5)
        
    def start_simulation(self):
        try:
            num_exp = int(self.entry.get())
            if num_exp <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Input Error", "Please enter a positive integer for experiments.")
            return

        self.run_button.config(state=tk.DISABLED)
        self.progress_bar['maximum'] = num_exp
        self.progress_bar['value'] = 0
        self.root.update()
        
        # Run simulation in a separate thread to keep GUI responsive
        self.root.after(100, lambda: self.run_experiments(num_exp))
        
    def run_experiments(self, num_experiments):
        self.lateness_values = []
        self.Max_Max_lateness = float('-inf')
        random.seed(42)
        np.random.seed(42)
        
        for week in range(1, num_experiments + 1):
            # Update progress
            self.progress_label.config(text=f"Running Experiment {week}/{num_experiments}")
            self.progress_bar['value'] = week
            self.root.update()
            
            # Run simulation
            max_lateness = simulate_over_time(self,
                total_hours=168,
                disruption_rates=(0.017, 0.0089, 0.0013)
            )
            self.lateness_values.append(max_lateness)
            self.Max_Max_lateness = max(self.Max_Max_lateness, max_lateness)
            
            # Update plot
            self.update_plot(week)
            
        # Final update
        self.average_max_lateness = sum(self.lateness_values) / len(self.lateness_values)
        self.summary_label.config(
            text=f"Average max lateness: {self.average_max_lateness:.2f}\nMaximum of max lateness: {self.Max_Max_lateness:.2f}"
        )
        self.progress_label.config(text="Simulation completed")
        self.run_button.config(state=tk.NORMAL)
        
    def update_plot(self, current_experiment):
        self.ax.clear()
        self.ax.plot(range(1, current_experiment + 1), self.lateness_values, marker='o', color='b')
        self.ax.set_title(f"Maximum Lateness per Experiment (Current: {current_experiment})")
        self.ax.set_xlabel("Experiment Number")
        self.ax.set_ylabel("Maximum Lateness")
        self.ax.grid(True)
        
        # Add horizontal line for average if we have more than 1 experiment
        if current_experiment > 1:
            current_avg = sum(self.lateness_values) / len(self.lateness_values)
            self.ax.axhline(y=current_avg, color='r', linestyle='--', 
                           label=f'Current Avg: {current_avg:.2f}')
            self.ax.legend()
        
        self.canvas.draw()
        
    def on_close(self):
        plt.close('all')
        self.root.destroy()
        sys.exit()
        os._exit(0)

if __name__ == "__main__":
    root = tk.Tk()
    app = SimulationGUI(root)
    root.mainloop()

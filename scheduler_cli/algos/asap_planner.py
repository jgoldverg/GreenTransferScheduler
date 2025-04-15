import pandas as pd
from .output import OutputFormatter  # Absolute import


class BruteForceGreenPlanner:
    def __init__(self, associations_df, jobs, nodes):
        self.associations_df = associations_df
        self.jobs = jobs
        self.nodes = nodes
        self.job_list = sorted(
            [job for job in jobs],
            key=lambda x: x['id']
        )
        self.columns = associations_df['node'].unique()
        self.rows = associations_df['forecast_id'].unique()
        self.remaining_capacity = {node: {} for node in self.columns}

        # Initialize OutputFormatter
        self.output_formatter = OutputFormatter(
            associations_df=self.associations_df,
            job_list=jobs,
            node_list=nodes,
            time_slots=sorted(associations_df['forecast_id'].unique())
        )

    def find_greenest_slots(self, node_name, job_duration):
        """Finds the greenest slots for a job on a given node."""
        node_forecasts = self.associations_df[self.associations_df['node'] == node_name]
        available_slots = []

        for slot in self.rows:
            ci = node_forecasts[node_forecasts['forecast_id'] == slot]['carbon_emissions'].values[0]
            available_slots.append((slot, ci))

        available_slots.sort(key=lambda x: x[1])
        allocated_slots = []
        total_allocated_time = 0

        for slot, _ in available_slots:
            if slot not in self.remaining_capacity[node_name]:
                self.remaining_capacity[node_name][slot] = 3600

            slot_time = min(self.remaining_capacity[node_name][slot], job_duration - total_allocated_time)
            if slot_time > 0:
                allocated_slots.append((slot, slot_time))
                total_allocated_time += slot_time
                self.remaining_capacity[node_name][slot] -= slot_time

            if total_allocated_time >= job_duration:
                break

        return allocated_slots if total_allocated_time >= job_duration else None

    def plan(self):
        schedule = []
        for job in self.job_list:
            best_node = None
            best_slots = None
            best_avg_ce = float('inf')
            job_duration = self.associations_df[
                self.associations_df['job_id'] == job['id']
                ]['transfer_time'].values[0]

            for node in self.columns:
                allocated_slots = self.find_greenest_slots(node, job_duration)
                if allocated_slots:
                    total_ce = 0
                    total_time = 0
                    for slot, slot_time in allocated_slots:
                        ce = self.associations_df[
                            (self.associations_df['node'] == node) &
                            (self.associations_df['forecast_id'] == slot)
                            ]['carbon_emissions'].values[0]
                        total_ce += ce * slot_time
                        total_time += slot_time
                    avg_ce = total_ce / total_time

                    if avg_ce < best_avg_ce:
                        best_avg_ce = avg_ce
                        best_node = node
                        best_slots = allocated_slots

            if best_slots:
                for slot, slot_time in best_slots:
                    carbon_emissions = self.associations_df[
                        (self.associations_df['job_id'] == job['id']) &
                        (self.associations_df['node'] == best_node) &
                        (self.associations_df['forecast_id'] == slot)
                        ]['carbon_emissions'].values[0]

                    # Include throughput for metrics calculation
                    throughput = self.associations_df[
                        (self.associations_df['job_id'] == job['id']) &
                        (self.associations_df['node'] == best_node) &
                        (self.associations_df['forecast_id'] == slot)
                        ]['throughput'].values[0]

                    schedule.append({
                        'idx': len(schedule),  # Add index for tracking
                        'job_id': job['id'],
                        'node': best_node,
                        'forecast_id': slot,
                        'allocated_time': slot_time,
                        'carbon_emissions': carbon_emissions,
                        'throughput': throughput  # Added for metrics
                    })

        schedule_df = pd.DataFrame(schedule)

        # Use OutputFormatter for consistent output handling
        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename='brute_force_green_schedule.csv',
            optimization_mode='green'  # Indicates this is a green-optimized planner
        )
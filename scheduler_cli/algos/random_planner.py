import random

import click
import numpy as np
import pandas as pd
from .output import OutputFormatter


class RandomPlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = node_list  # Keep full node objects for OutputFormatter
        self.job_list = job_list  # Keep full job objects
        self.time_slots = sorted(associations_df['forecast_id'].unique())

        # Initialize OutputFormatter
        self.output_formatter = OutputFormatter(
            associations_df=self.associations_df,
            job_list=job_list,
            node_list=node_list,
            time_slots=self.time_slots
        )

        # Track remaining capacity for each node and time slot
        node_names = [node['name'] for node in node_list]
        self.remaining_capacity = {
            node['name']: {slot: 3600 for slot in self.time_slots}
            for node in node_list
        }

        # Precompute metrics for all combinations
        self.metrics = {
            (int(row.job_id), int(row.forecast_id), row.node): {
                'carbon': row.carbon_emissions,
                'throughput': row.throughput
            }
            for _, row in associations_df.iterrows()
        }

    def get_transfer_time(self, job_id):
        """Retrieve the transfer_time for a job from the associations_df."""
        return self.associations_df[
            self.associations_df['job_id'] == job_id
            ]['transfer_time'].values[0]

    def plan(self):
        schedule = []
        job_ids = [job['id'] for job in self.job_list]
        random.shuffle(job_ids)  # Randomize job order

        for job_id in job_ids:
            job_duration = self.get_transfer_time(job_id)
            required_slots = int(np.ceil(job_duration / 3600))
            assigned = False
            max_attempts = 100
            attempts = 0

            while not assigned and attempts < max_attempts:
                node = random.choice(self.node_list)
                node_name = node['name']

                if required_slots > 1:
                    start_slot = random.choice(self.time_slots[:-required_slots + 1])
                else:
                    start_slot = random.choice(self.time_slots)

                # Check capacity in consecutive slots
                if all(
                        self.remaining_capacity[node_name][start_slot + i] >= min(3600, job_duration - i * 3600)
                        for i in range(required_slots)
                ):
                    # Assign the job
                    for i in range(required_slots):
                        slot = start_slot + i
                        duration = min(3600, job_duration - i * 3600)

                        # Get metrics for this assignment
                        metrics = self.metrics.get((job_id, slot, node_name), {})

                        schedule.append({
                            'idx': len(schedule),
                            'job_id': job_id,
                            'node': node_name,
                            'forecast_id': slot,
                            'allocated_time': duration,
                            'carbon_emissions': metrics.get('carbon', 0),
                            'throughput': metrics.get('throughput', 0)
                        })

                        # Update capacity
                        self.remaining_capacity[node_name][slot] -= duration
                    assigned = True
                attempts += 1

            if not assigned:
                click.secho(
                    f"Warning: No feasible assignment found for job {job_id} (duration: {job_duration}s). Skipping.",
                    fg='yellow'
                )

        schedule_df = pd.DataFrame(schedule)

        # Use OutputFormatter for consistent output handling
        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename='random_schedule.csv',
            optimization_mode='random'
        )

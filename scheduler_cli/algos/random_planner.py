import random
import numpy as np
import pandas as pd


class RandomPlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = [node['name'] for node in node_list]  # Extract node names
        self.job_list = [job['id'] for job in job_list]  # Extract job IDs
        self.time_slots = sorted(associations_df['forecast_id'].unique())

        # Track remaining capacity for each node and time slot
        self.remaining_capacity = {node: {slot: 3600 for slot in self.time_slots} for node in self.node_list}

        # Carbon emissions for each job, time slot, and node combination
        self.emissions = {
            (int(row.job_id), int(row.forecast_id), row.node): row.carbon_emissions
            for _, row in associations_df.iterrows()
        }

    def get_transfer_time(self, job_id):
        """
        Retrieve the transfer_time for a job from the associations_df.
        """
        transfer_time = self.associations_df[
            self.associations_df['job_id'] == job_id
            ]['transfer_time'].values[0]
        return transfer_time

    def plan(self):
        schedule = []  # To store the final schedule

        # Shuffle jobs to randomize assignment order
        random.shuffle(self.job_list)

        for job in self.job_list:
            # Get job duration from associations_df
            job_duration = self.get_transfer_time(job)

            # Calculate the number of time slots required for the job
            required_slots = int(np.ceil(job_duration / 3600))

            # Find a feasible node and consecutive time slots for the job
            assigned = False
            max_attempts = 100  # Limit the number of attempts to find a feasible assignment
            attempts = 0

            while not assigned and attempts < max_attempts:
                # Randomly select a node
                node = random.choice(self.node_list)

                # Randomly select a starting time slot
                # Ensure there are enough consecutive slots for the job
                if required_slots > 1:
                    # Only choose starting slots that allow for the required number of consecutive slots
                    start_slot = random.choice(self.time_slots[:-required_slots + 1])
                else:
                    start_slot = random.choice(self.time_slots)

                # Check if the required consecutive time slots are available
                if all(
                        self.remaining_capacity[node][start_slot + i] >= min(3600, job_duration - i * 3600)
                        for i in range(required_slots)
                ):
                    # Assign the job to the consecutive time slots
                    for i in range(required_slots):
                        slot = start_slot + i
                        duration = min(3600, job_duration - i * 3600)
                        schedule.append({
                            'job_id': job,
                            'node': node,
                            'forecast_id': slot,
                            'allocated_time': duration,
                            'carbon_emissions': self.emissions.get((job, slot, node), 0)
                        })
                        # Update remaining capacity
                        self.remaining_capacity[node][slot] -= duration
                    assigned = True
                attempts += 1

            if not assigned:
                print(
                    f"Warning: No feasible assignment found for job {job} (duration: {job_duration} seconds). Skipping.")

        # Convert schedule to DataFrame
        schedule_df = pd.DataFrame(schedule)
        schedule_df.reset_index(inplace=True)  # Add idx column
        schedule_df.rename(columns={'index': 'idx'}, inplace=True)

        # Save and return the schedule
        schedule_df.to_csv('/workspace/schedules/random_planner.csv', index=False)
        return schedule_df

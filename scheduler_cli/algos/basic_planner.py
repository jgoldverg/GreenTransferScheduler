import pandas as pd


class BasicPlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list
        self.slot_capacity = 3600  # Each time slot has a fixed capacity (3600 seconds)
        self.time_slots = sorted(associations_df['forecast_id'].unique())  # Available time slots
        self.remaining_capacity = {slot: self.slot_capacity for slot in self.time_slots}  # Track remaining capacity

    def get_transfer_time(self, job_id):
        """
        Retrieve the transfer_time for a job from the associations_df.
        """
        transfer_time = self.associations_df[
            self.associations_df['job_id'] == job_id
            ]['transfer_time'].values[0]
        return transfer_time

    def plan(self):
        schedule = []
        current_slot_index = 0  # Track the current time slot
        node_index = 0  # Track the current node index

        for job in self.job_list:
            job_id = job['id']
            job_duration = self.get_transfer_time(job_id)  # Get transfer_time from associations_df
            remaining_job_duration = job_duration

            # Select the next node in the list (cycle back to the first node if necessary)
            selected_node = self.node_list[node_index]['name']
            node_index = (node_index + 1) % len(self.node_list)  # Cycle through nodes

            while remaining_job_duration > 0:
                if current_slot_index >= len(self.time_slots):
                    raise ValueError("Not enough time slots to schedule all jobs.")

                current_slot = self.time_slots[current_slot_index]
                allocated_time = min(remaining_job_duration, self.remaining_capacity[current_slot])

                # Look up carbon emissions for this job, node, and time slot
                carbon_emissions = self.associations_df[
                    (self.associations_df['job_id'] == job_id) &
                    (self.associations_df['node'] == selected_node) &
                    (self.associations_df['forecast_id'] == current_slot)
                    ]['carbon_emissions'].values[0]

                schedule.append({
                    'job_id': job_id,
                    'node': selected_node,
                    'forecast_id': current_slot,
                    'allocated_time': allocated_time,
                    'carbon_emissions': carbon_emissions
                })

                remaining_job_duration -= allocated_time
                self.remaining_capacity[current_slot] -= allocated_time

                if self.remaining_capacity[current_slot] == 0:
                    # Move to the next slot
                    current_slot_index += 1

        # Convert the schedule to a DataFrame
        schedule_df = pd.DataFrame(schedule)
        schedule_df.reset_index(inplace=True)  # Add idx column
        schedule_df.rename(columns={'index': 'idx'}, inplace=True)

        # Save and return the schedule
        schedule_df.to_csv('/workspace/schedules/basic_planner.csv', index=False)
        return schedule_df

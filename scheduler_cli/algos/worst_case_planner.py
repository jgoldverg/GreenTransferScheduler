import pandas as pd


class WorstCasePlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = [node['name'] for node in node_list]  # Extract node names
        self.job_list = job_list
        self.columns = self.associations_df['node'].unique()
        self.rows = self.associations_df['forecast_id'].unique()
        self.remaining_capacity = {node: {slot: 3600 for slot in self.rows} for node in
                                   self.columns}  # Track available seconds per slot

    def get_transfer_time(self, job_id):
        """
        Retrieve the transfer_time for a job from the associations_df.
        """
        transfer_time = self.associations_df[
            self.associations_df['job_id'] == job_id
            ]['transfer_time'].values[0]
        return transfer_time

    def find_worst_slots(self, node_name, job_duration):
        """
        Finds multiple non-contiguous slots with the highest carbon emissions to fit the job.
        """
        node_forecasts = self.associations_df[self.associations_df['node'] == node_name]
        available_slots = []

        for slot in self.rows:
            if self.remaining_capacity[node_name][slot] >= min(3600, job_duration):
                emissions = node_forecasts[node_forecasts['forecast_id'] == slot]['carbon_emissions'].values[0]
                available_slots.append((slot, emissions))

        # Sort by carbon emissions (descending) to prioritize worst-case slots
        available_slots.sort(key=lambda x: x[1], reverse=True)

        allocated_slots = []
        total_allocated_time = 0

        for slot, _ in available_slots:
            slot_time = min(3600, job_duration - total_allocated_time)
            allocated_slots.append((slot, slot_time))
            total_allocated_time += slot_time
            self.remaining_capacity[node_name][slot] -= slot_time

            if total_allocated_time >= job_duration:
                break

        return allocated_slots if total_allocated_time >= job_duration else None

    def plan(self):
        schedule = []  # List to store the schedule

        for job in self.job_list:
            job_id = job['id']
            job_duration = self.get_transfer_time(job_id)  # Get transfer_time from associations_df

            best_node = None
            best_slots = None
            best_total_emissions = -1  # Track the highest total emissions

            for node in self.columns:
                allocated_slots = self.find_worst_slots(node, job_duration)
                if allocated_slots is not None:
                    # Calculate the total carbon emissions for the allocated slots
                    total_emissions = sum(
                        self.associations_df[
                            (self.associations_df['node'] == node) &
                            (self.associations_df['forecast_id'] == slot)
                            ]['carbon_emissions'].values[0] * slot_time
                        for slot, slot_time in allocated_slots
                    )

                    if total_emissions > best_total_emissions:
                        best_total_emissions = total_emissions
                        best_node = node
                        best_slots = allocated_slots

            if best_slots is not None:
                for slot, slot_time in best_slots:
                    # Look up carbon emissions for this allocation
                    carbon_emissions = self.associations_df[
                        (self.associations_df['job_id'] == job_id) &
                        (self.associations_df['node'] == best_node) &
                        (self.associations_df['forecast_id'] == slot)
                        ]['carbon_emissions'].values[0]

                    # Add the allocation to the schedule
                    schedule.append({
                        'job_id': job_id,
                        'node': best_node,
                        'forecast_id': slot,
                        'allocated_time': slot_time,
                        'carbon_emissions': carbon_emissions
                    })

        # Convert the schedule to a DataFrame
        schedule_df = pd.DataFrame(schedule)
        schedule_df.reset_index(inplace=True)  # Add idx column
        schedule_df.rename(columns={'index': 'idx'}, inplace=True)

        # Save and return the schedule
        schedule_df.to_csv('/workspace/schedules/worst_case_schedule.csv', index=False)
        return schedule_df

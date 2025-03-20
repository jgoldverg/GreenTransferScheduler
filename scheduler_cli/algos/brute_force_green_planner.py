import pandas as pd


class BruteForceGreenPlanner:
    def __init__(self, associations_df, jobs, nodes):
        self.associations_df = associations_df
        self.jobs = jobs
        self.nodes = nodes
        self.job_list = sorted(
            [job for job in jobs],  # Sort jobs by ID or other criteria
            key=lambda x: x['id']
        )
        self.columns = associations_df['node'].unique()
        self.rows = associations_df['forecast_id'].unique()
        self.remaining_capacity = {node: {} for node in self.columns}  # Track available time intervals per node

    def find_greenest_slots(self, node_name, job_duration):
        """Finds the greenest slots for a job on a given node."""
        node_forecasts = self.associations_df[self.associations_df['node'] == node_name]
        available_slots = []

        # Get all time intervals for the node
        for slot in self.rows:
            ci = node_forecasts[node_forecasts['forecast_id'] == slot]['carbon_emissions'].values[0]
            available_slots.append((slot, ci))

        # Sort slots by carbon intensity (lowest first)
        available_slots.sort(key=lambda x: x[1])

        allocated_slots = []
        total_allocated_time = 0

        for slot, _ in available_slots:
            # Check if the slot is already allocated
            if slot not in self.remaining_capacity[node_name]:
                self.remaining_capacity[node_name][slot] = 3600  # Initialize slot capacity

            # Allocate as much time as possible in this slot (up to remaining job duration)
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
            job_duration = self.associations_df[self.associations_df['job_id'] == job['id']]['transfer_time'].values[0]

            # Evaluate all nodes (shifting in space)
            for node in self.columns:
                # Evaluate all possible time slots (shifting in time)
                allocated_slots = self.find_greenest_slots(node, job_duration)
                if allocated_slots is not None:
                    # Calculate the average carbon emissions for the allocated slots
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

                    # Update the best allocation if this one is greener
                    if avg_ce < best_avg_ce:
                        best_avg_ce = avg_ce
                        best_node = node
                        best_slots = allocated_slots

            if best_slots is not None:
                for slot, slot_time in best_slots:
                    # Look up carbon emissions for this allocation
                    carbon_emissions = self.associations_df[
                        (self.associations_df['job_id'] == job['id']) &
                        (self.associations_df['node'] == best_node) &
                        (self.associations_df['forecast_id'] == slot)
                        ]['carbon_emissions'].values[0]

                    # Add the allocation to the schedule
                    schedule.append({
                        'job_id': job['id'],
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
        schedule_df.to_csv('/workspace/schedules/green_schedule.csv', index=False)
        return schedule_df

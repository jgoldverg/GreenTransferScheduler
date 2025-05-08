from typing import List, Dict
import click
import pandas as pd


class RoundRobin:
    def __init__(self, associations_df: pd.DataFrame, job_list: List[Dict]):
        self.df = associations_df
        self.job_list = job_list
        self.routes = associations_df['route_key'].unique()
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])
        self.capacity = {
            route_key: {slot: 3600.0 for slot in self.time_slots}
            for route_key in self.routes
        }
        # Precompute transfer times for each job-node pair
        self.transfer_times = self.df.groupby(['job_id', 'route_key'])['transfer_time'].first().to_dict()


        # Round-robin state
        self.next_node_idx = 0

    def _get_next_route_key(self):
        """Get next node in round-robin order"""
        node = self.routes[self.next_node_idx % len(self.routes)]
        self.next_node_idx += 1
        return node

    def _get_job_transfer_time(self, job_id, route_key):
        """Get transfer time for a job-node pair"""
        return self.transfer_times.get((job_id, route_key), 0)

    def _find_available_slots(self, job_id, route_key, deadline):
        """Find available slots for a job considering deadline"""
        transfer_time = self._get_job_transfer_time(job_id, route_key)
        if transfer_time == 0:
            return []  # Job can't run on this node

        try:
            deadline = int(deadline) if deadline is not None else None
        except (ValueError, TypeError):
            deadline = None

        max_slot = int(max(self.time_slots))
        deadline_slot = min(deadline, max_slot) if deadline is not None else max_slot

        # Calculate total needed capacity
        total_needed = transfer_time
        allocated = 0
        slot_indices = []

        # Iterate through slots in order until we meet the requirement or hit deadline
        for slot_idx, slot_time in enumerate(self.time_slots):
            if int(slot_time) > deadline_slot:
                break

            available = min(self.capacity[route_key][slot_time], total_needed - allocated)
            if available > 0:
                slot_indices.append(slot_idx)
                allocated += available

            if allocated >= total_needed:
                break

        if allocated >= total_needed:
            return slot_indices
        return []

    def plan(self):
        """Generate pure round-robin schedule"""
        schedule = []

        # Sort jobs by deadline (earliest first)
        jobs_sorted = sorted(self.job_list,
                             key=lambda x: (float('inf') if x['deadline'] is None
                                            else int(x['deadline'])))

        for idx, job in enumerate(jobs_sorted):
            job_id = job['id']
            deadline = job['deadline']
            scheduled = False

            # Try all nodes in round-robin order
            for _ in range(len(self.routes)):
                route_key = self._get_next_route_key()
                slot_indices = self._find_available_slots(job_id, route_key, deadline)

                if not slot_indices:
                    continue

                # Get slot metrics from dataframe
                slot_data = self.df[(self.df['route_key'] == route_key) &
                                    (self.df['job_id'] == job_id)].iloc[0]

                transfer_time = self._get_job_transfer_time(job_id, route_key)
                remaining_time = transfer_time

                # Allocate time to each slot
                for slot_idx in slot_indices:
                    slot_time = self.time_slots[slot_idx]
                    allocate = min(self.capacity[route_key][slot_time], remaining_time)

                    self.capacity[route_key][slot_time] -= allocate
                    remaining_time -= allocate

                    schedule.append({
                        'job_id': job['id'],
                        'route_key': route_key,
                        'forecast_id': slot_time,
                        'allocated_time': allocate,
                        'carbon_emissions': slot_data['carbon_emissions'],
                        'throughput': slot_data['throughput'],
                        'transfer_time': allocate,  # This is the actual allocated time
                        'deadline': job.get('deadline'),
                    })

                    if remaining_time <= 0:
                        break

                scheduled = True
                break

            if not scheduled:
                click.secho(f"⚠️ Failed to schedule Job {job_id} (deadline: {deadline})", fg='yellow')

        # Format output
        return pd.DataFrame(schedule)

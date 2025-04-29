from typing import List, Dict
import pandas as pd


class CarbonAwarePlanner:
    def __init__(self, associations_df: pd.DataFrame, jobs: List[Dict],
                 mode: str = 'min'):
        self.df = associations_df
        self.mode = mode.lower()
        self.job_list = jobs
        self.reverse_sort = (self.mode == 'max')

        # Initialize capacity tracking
        self.nodes = associations_df['node'].unique()
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])
        self.capacity = {
            node: {slot: 3600.0 for slot in self.time_slots}
            for node in self.nodes
        }

        # Sort jobs by deadline then carbon priority
        self.jobs = sorted(
            jobs,
            key=lambda x: (x.get('deadline', float('inf')),
                           self._get_job_emissions(x['id'])),
            reverse=self.reverse_sort
        )

    def _get_job_emissions(self, job_id: str) -> float:
        """Get best-case/worst-case emissions for a job"""
        job_emissions = self.df[self.df['job_id'] == job_id]['carbon_emissions']
        return job_emissions.max() if self.reverse_sort else job_emissions.min()

    def plan(self) -> pd.DataFrame:
        schedule = []
        unallocated_jobs = []

        for job in self.jobs:
            if not self._allocate_job(job, schedule):
                unallocated_jobs.append(job['id'])

        return pd.DataFrame(schedule)

    def _allocate_job(self, job: Dict, schedule: List) -> bool:
        job_df = self.df[self.df['job_id'] == job['id']]
        deadline = job.get('deadline', float('inf'))

        # Get all possible allocations before deadline
        valid_allocations = job_df[job_df['forecast_id'] <= deadline]
        if valid_allocations.empty:
            return False

        # Sort by carbon preference
        allocations = valid_allocations.sort_values(
            'carbon_emissions',
            ascending=not self.reverse_sort
        )

        # Try to allocate in the greenest possible slots
        remaining_time = None
        allocated_slots = []

        for _, slot in allocations.iterrows():
            node = slot['node']
            slot_id = slot['forecast_id']
            node_specific_time = slot['transfer_time']

            # Initialize remaining_time on first iteration
            if remaining_time is None:
                remaining_time = node_specific_time

            if self.capacity[node][slot_id] > 0:
                alloc = min(self.capacity[node][slot_id], remaining_time)
                self.capacity[node][slot_id] -= alloc
                allocated_slots.append({
                    'node': node,
                    'slot_id': slot_id,
                    'time_used': alloc,
                    'slot_data': slot
                })
                remaining_time -= alloc

                if remaining_time <= 0:
                    # Job fully allocated
                    for alloc in allocated_slots:
                        self._add_schedule_entry(job, alloc, schedule)
                    return True

        # If we get here, allocation failed - roll back
        for alloc in allocated_slots:
            self.capacity[alloc['node']][alloc['slot_id']] += alloc['time_used']
        return False

    def _add_schedule_entry(self, job: Dict, allocation: Dict, schedule: List):
        """Add an allocation to the schedule"""
        schedule.append({
            'job_id': job['id'],
            'node': allocation['node'],
            'forecast_id': allocation['slot_id'],
            'allocated_time': allocation['time_used'],
            'carbon_emissions': allocation['slot_data']['carbon_emissions'],
            'throughput': allocation['slot_data']['throughput'],
            'transfer_time': allocation['slot_data']['transfer_time'],
            'deadline': job.get('deadline')
        })
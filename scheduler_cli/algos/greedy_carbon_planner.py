from typing import List, Dict
import pandas as pd


class CarbonAwarePlanner:
    def __init__(self, associations_df: pd.DataFrame, jobs: List[Dict], mode: str = 'min'):
        self.df = associations_df
        self.mode = mode.lower()
        self.job_list = jobs
        self.reverse_sort = (self.mode == 'max')

        # Initialize capacity tracking (in seconds)
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])
        self.capacity = {
            route_key: {slot: 3600.0 for slot in self.time_slots}
            for route_key in associations_df['route_key'].unique()
        }

        # Precompute job metrics
        self.job_metrics = self._precompute_job_metrics()
        self.job_deadlines = {job['id']: job.get('deadline') for job in jobs}

        # Sort jobs by deadline (earliest first)
        self.jobs = sorted(
            jobs,
            key=lambda x: int(x['deadline']) if x.get('deadline') is not None else float('inf')
        )

    def _precompute_job_metrics(self):
        """Precompute metrics for each job-route pair"""
        metrics = {}
        for (job_id, route_key), group in self.df.groupby(['job_id', 'route_key']):
            if job_id not in metrics:
                metrics[job_id] = {}
            first_row = group.iloc[0]
            total_transfer_time = float(first_row['transfer_time_hours']) * 3600  # in seconds
            metrics[job_id][route_key] = {
                'source_node': first_row['source_node'],
                'destination_node': first_row['destination_node'],
                'carbon_emissions': float(first_row['carbon_emissions']),
                'throughput': float(first_row['throughput']),
                'transfer_time': total_transfer_time,
                'transfer_time_hours': float(first_row['transfer_time_hours'])
            }
        return metrics

    def plan(self) -> pd.DataFrame:
        """Generate carbon-aware schedule"""
        schedule = []
        unallocated_jobs = []

        for job in self.jobs:
            if not self._allocate_job(job, schedule):
                unallocated_jobs.append(job['id'])

        if unallocated_jobs:
            print(f"Warning: Failed to schedule jobs: {unallocated_jobs}")

        return pd.DataFrame(schedule)

    def _allocate_job(self, job: Dict, schedule: List) -> bool:
        job_id = job['id']
        if job_id not in self.job_metrics:
            return False

        deadline = self.job_deadlines[job_id]
        routes = self.job_metrics[job_id]

        # Sort routes by carbon preference
        routes_sorted = sorted(
            routes.items(),
            key=lambda x: x[1]['carbon_emissions'],
            reverse=self.reverse_sort
        )

        for route_key, metrics in routes_sorted:
            remaining_time = metrics['transfer_time']
            allocated_slots = []

            # Find all available slots before deadline
            for slot_id in sorted(self.time_slots):
                if deadline is not None and slot_id > deadline:
                    continue

                if remaining_time <= 0:
                    break

                available = self.capacity[route_key][slot_id]
                if available > 0:
                    alloc = min(available, remaining_time)
                    self.capacity[route_key][slot_id] -= alloc
                    allocated_slots.append({
                        'slot_id': slot_id,
                        'time_used': alloc,
                        'metrics': metrics
                    })
                    remaining_time -= alloc

            if remaining_time <= 0:
                # Job fully allocated - add to schedule
                for alloc in allocated_slots:
                    self._add_schedule_entry(
                        job,
                        route_key,
                        alloc['slot_id'],
                        alloc['time_used'],
                        alloc['metrics'],
                        schedule
                    )
                return True
            else:
                # Roll back partial allocation
                for alloc in allocated_slots:
                    self.capacity[route_key][alloc['slot_id']] += alloc['time_used']

        return False

    def _add_schedule_entry(self, job: Dict, route_key: str, slot_id: int,
                          time_used: float, metrics: Dict, schedule: List):
        """Add an allocation to the schedule"""
        allocated_fraction = time_used / 3600  # Fraction of slot used

        schedule.append({
            'job_id': job['id'],
            'route': route_key,
            'source_node': metrics['source_node'],
            'destination_node': metrics['destination_node'],
            'forecast_id': slot_id,
            'allocated_fraction': allocated_fraction,
            'allocated_time': time_used,
            'carbon_emissions': metrics['carbon_emissions'],  # Full emissions for job
            'throughput': metrics['throughput'],
            'transfer_time': metrics['transfer_time'],  # Total job time
            'deadline': self.job_deadlines[job['id']],
            'extendable': job.get('extendable', False)
        })
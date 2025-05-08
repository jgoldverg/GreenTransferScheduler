from collections import defaultdict
from typing import List, Dict
import click
import pandas as pd


class ShortestJobFirst:
    def __init__(self, associations_df: pd.DataFrame, job_list: List[Dict]):
        self.df = associations_df
        self.job_list = job_list
        self.routes = associations_df['route_key'].unique()
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])
        self.capacity = {
            route: {slot: 3600.0 for slot in self.time_slots}
            for route in self.routes
        }

        # Precompute transfer times and other metrics
        self.job_metrics = self._precompute_job_metrics()


    def _precompute_job_metrics(self):
        """Precompute metrics for each job-node pair"""
        metrics = defaultdict(dict)
        for (job_id, route_key), group in self.df.groupby(['job_id', 'route_key']):
            first_row = group.iloc[0]
            metrics[job_id][route_key] = {
                'transfer_time': float(first_row['transfer_time']),
                'carbon_emissions': float(first_row.get('carbon_emissions')),
                'throughput': float(first_row.get('throughput'))
            }
        return metrics

    def _get_job_metrics(self, job_id, route_key):
        """Get precomputed metrics for a job-node pair"""
        return self.job_metrics.get(job_id, {}).get(route_key)

    def _find_available_slots(self, job_id, route_key, deadline):
        """Find available slots for a job considering deadline"""
        metrics = self._get_job_metrics(job_id, route_key)
        if not metrics or metrics['transfer_time'] <= 0:
            return []  # Job can't run on this node

        transfer_time = metrics['transfer_time']
        slots_needed = max(1, (int(transfer_time) + 3599) // 3600)  # Ensure at least 1 slot

        # Handle deadline
        try:
            deadline = int(deadline) if deadline is not None else None
        except (ValueError, TypeError):
            deadline = None

        max_slot = max(self.time_slots)
        deadline_slot = min(deadline, max_slot) if deadline is not None else max_slot

        # Try to find consecutive slots
        for start_slot in range(len(self.time_slots) - slots_needed + 1):
            end_slot = start_slot + slots_needed - 1
            if self.time_slots[end_slot] > deadline_slot:
                continue

            # Check if all needed slots have enough capacity
            can_allocate = True
            time_per_slot = transfer_time / slots_needed
            for slot in range(start_slot, end_slot + 1):
                if self.capacity[route_key][self.time_slots[slot]] < time_per_slot:
                    can_allocate = False
                    break

            if can_allocate:
                return list(range(start_slot, end_slot + 1))

        return []

    def _add_entry(self, job: Dict, route_key: str, slot_time: int, metrics: Dict, alloc: float, schedule: List):
        """Helper method to add an entry to the schedule"""
        self.capacity[route_key][slot_time] -= alloc
        schedule.append({
            'job_id': job['id'],
            'node': route_key,
            'forecast_id': slot_time,
            'allocated_time': alloc,
            'carbon_emissions': metrics['carbon_emissions'],
            'throughput': metrics['throughput'],
            'transfer_time': alloc,  # Divided transfer_time across slots
            'deadline': job.get('deadline'),
        })

    def plan(self):
        """Generate true SJF schedule (shortest transfer time first) with deadline as tiebreaker"""
        schedule = []

        # Sort jobs by: 1) shortest transfer time, 2) earliest deadline
        jobs_sorted = sorted(
            self.job_list,
            key=lambda x: (
                min(
                    [m['transfer_time'] for m in self.job_metrics.get(x['id'], {}).values()],
                    default=float('inf')
                ),
                int(x['deadline']) if x.get('deadline') is not None else float('inf')
            )
        )

        for idx, job in enumerate(jobs_sorted):
            job_id = job['id']
            deadline = job.get('deadline')
            scheduled = False

            # Get all possible nodes for this job, sorted by transfer time (fastest first)
            node_options = sorted(
                [route_key for route_key in self.routes if self._get_job_metrics(job_id, route_key)],
                key=lambda route_key: self._get_job_metrics(job_id, route_key)['transfer_time']
            )

            for node in node_options:
                slot_indices = self._find_available_slots(job_id, node, deadline)
                if not slot_indices:
                    continue

                metrics = self._get_job_metrics(job_id, node)
                transfer_time = metrics['transfer_time']
                time_per_slot = transfer_time / len(slot_indices)

                for slot_idx in slot_indices:
                    slot_time = self.time_slots[slot_idx]
                    self._add_entry(job, node, slot_time, metrics, time_per_slot, schedule)

                scheduled = True
                break

            if not scheduled:
                click.secho(f"⚠️ Failed to schedule Job {job_id} (deadline: {deadline})", fg='yellow')

        return pd.DataFrame(schedule)

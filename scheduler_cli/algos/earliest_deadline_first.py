from collections import defaultdict
from typing import List, Dict
import click
import pandas as pd
import math


class EarliestDeadlineFirst:
    def __init__(self, associations_df: pd.DataFrame, job_list: List[Dict]):
        self.df = associations_df
        self.job_list = job_list

        # Get unique time slots from forecast_id
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])

        # Capacity is now per route_key
        self.capacity = {
            row['route_key']: {slot: 3600.0 for slot in self.time_slots}
            for _, row in associations_df.drop_duplicates(['route_key']).iterrows()
        }

        # Precompute job metrics for each route
        self.job_metrics = self._precompute_job_metrics()

    def _precompute_job_metrics(self):
        """Precompute metrics for each job-route pair"""
        metrics = defaultdict(dict)
        for (job_id, route_key), group in self.df.groupby(['job_id', 'route_key']):
            first_row = group.iloc[0]
            metrics[job_id][route_key] = {
                'transfer_time': float(first_row['transfer_time']),
                'carbon_emissions': float(first_row.get('carbon_emissions', 0)),
                'throughput': float(first_row.get('throughput', 0)),
                'transfer_time_hours': float(first_row.get('transfer_time_hours', 0)),
                'source_node': first_row['source_node'],
                'destination_node': first_row['destination_node']
            }
        return metrics

    def _get_job_metrics(self, job_id, route_key):
        """Get metrics for a specific job-route pair"""
        return self.job_metrics.get(job_id, {}).get(route_key)

    def _find_available_slots(self, job_id, route_key, deadline):
        """Find consecutive slots that can accommodate the job before deadline"""
        metrics = self._get_job_metrics(job_id, route_key)
        if not metrics or metrics['transfer_time'] <= 0:
            return []

        transfer_time = metrics['transfer_time_hours'] * 3600  # Convert hours to seconds
        slots_needed = math.ceil(transfer_time / 3600)  # Round up to full slots

        # Handle deadline
        try:
            deadline = int(deadline) if deadline is not None else None
        except (ValueError, TypeError):
            deadline = None

        max_slot = max(self.time_slots)
        deadline_slot = min(deadline, max_slot) if deadline is not None else max_slot

        # Find earliest possible slots that meet deadline
        for start_slot_idx in range(len(self.time_slots) - slots_needed + 1):
            end_slot_idx = start_slot_idx + slots_needed - 1
            if self.time_slots[end_slot_idx] > deadline_slot:
                continue  # Doesn't meet deadline

            # Check capacity in all required slots
            can_allocate = all(
                self.capacity[route_key][self.time_slots[slot_idx]] >= transfer_time / slots_needed
                for slot_idx in range(start_slot_idx, end_slot_idx + 1)
            )

            if can_allocate:
                return list(range(start_slot_idx, end_slot_idx + 1))

        return []

    def _add_schedule_entry(self, job, route_key, slot_indices, metrics, schedule):
        """Add an entry to the schedule and update capacities"""
        transfer_time = metrics['transfer_time_hours'] * 3600  # Convert hours to seconds
        time_per_slot = transfer_time / len(slot_indices)
        allocated_fraction = time_per_slot / 3600  # Fraction of the hour slot used

        for slot_idx in slot_indices:
            slot_time = self.time_slots[slot_idx]
            self.capacity[route_key][slot_time] -= time_per_slot

            schedule.append({
                'job_id': job['id'],
                'route': route_key,
                'source_node': metrics['source_node'],
                'destination_node': metrics['destination_node'],
                'forecast_id': slot_time,
                'allocated_fraction': allocated_fraction,
                'allocated_time': time_per_slot,
                'carbon_emissions': metrics['carbon_emissions'],
                'throughput': metrics['throughput'],
                'transfer_time': metrics['transfer_time'],  # Total job time
                'deadline': job.get('deadline'),
                'extendable': job.get('extendable', False)
            })

    def plan(self):
        """Generate pure EDF schedule (deadline is only priority)"""
        schedule = []

        # Sort jobs STRICTLY by deadline (earliest first)
        jobs_sorted = sorted(
            self.job_list,
            key=lambda x: int(x['deadline']) if x.get('deadline') is not None else float('inf')
        )

        for idx, job in enumerate(jobs_sorted):
            job_id = job['id']
            deadline = job.get('deadline')
            scheduled = False

            # Try all possible routes for this job
            for route_key in self.job_metrics.get(job_id, {}).keys():
                slot_indices = self._find_available_slots(job_id, route_key, deadline)
                if slot_indices:
                    metrics = self._get_job_metrics(job_id, route_key)
                    self._add_schedule_entry(job, route_key, slot_indices, metrics, schedule)
                    scheduled = True
                    break

            if not scheduled:
                click.secho(f"‼️ URGENT: Failed to schedule Job {job_id} (deadline: {deadline})", fg='red')

        return pd.DataFrame(schedule)
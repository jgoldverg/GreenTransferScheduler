from typing import List, Dict
import click
import pandas as pd


class RoundRobin:
    def __init__(self, associations_df: pd.DataFrame, job_list: List[Dict]):
        self.df = associations_df
        self.job_list = job_list
        self.routes = associations_df['route_key'].unique()
        self.time_slots = sorted([int(x) for x in associations_df['forecast_id'].unique()])

        # Initialize capacity in seconds (3600 per slot)
        self.capacity = {
            route_key: {slot: 3600.0 for slot in self.time_slots}
            for route_key in self.routes
        }

        # Precompute job metrics
        self.job_metrics = self._precompute_job_metrics()
        self.job_deadlines = {job['id']: job.get('deadline') for job in job_list}

        # Round-robin state
        self.next_node_idx = 0

    def _precompute_job_metrics(self):
        """Precompute metrics for each job-route pair"""
        metrics = {}
        for (job_id, route_key), group in self.df.groupby(['job_id', 'route_key']):
            if job_id not in metrics:
                metrics[job_id] = {}
            first_row = group.iloc[0]
            metrics[job_id][route_key] = {
                'source_node': first_row['source_node'],
                'destination_node': first_row['destination_node'],
                'carbon': float(first_row['carbon_emissions']),
                'throughput': float(first_row['throughput']),
                'transfer_time': float(first_row['transfer_time_hours']) * 3600,  # in seconds
                'transfer_time_hours': float(first_row['transfer_time_hours']),
            }
        return metrics

    def _get_next_route_key(self):
        """Get next route in round-robin order"""
        route_key = self.routes[self.next_node_idx % len(self.routes)]
        self.next_node_idx += 1
        return route_key

    def _get_job_metrics(self, job_id, route_key):
        """Get metrics for a specific job-route pair"""
        return self.job_metrics.get(job_id, {}).get(route_key)

    def _find_available_slots(self, job_id, route_key, deadline):
        """Find available slots for a job considering deadline"""
        metrics = self._get_job_metrics(job_id, route_key)
        if not metrics or metrics['transfer_time'] <= 0:
            return []

        try:
            deadline = int(deadline) if deadline is not None else None
        except (ValueError, TypeError):
            deadline = None

        max_slot = max(self.time_slots)
        deadline_slot = min(deadline, max_slot) if deadline is not None else max_slot

        needed_seconds = metrics['transfer_time']
        allocated = 0.0
        slot_allocations = []

        for slot_idx, slot_time in enumerate(self.time_slots):
            if slot_time > deadline_slot:
                break

            available = self.capacity[route_key][slot_time]
            alloc_seconds = min(available, needed_seconds - allocated)
            if alloc_seconds > 0:
                alloc_fraction = alloc_seconds / metrics['transfer_time']
                slot_allocations.append((slot_idx, alloc_fraction, alloc_seconds))
                allocated += alloc_seconds

            if allocated >= needed_seconds:
                break

        return slot_allocations if allocated >= needed_seconds else []

    def plan(self):
        """Generate round-robin schedule matching MILP output format"""
        schedule = []

        # Sort jobs by deadline (earliest first)
        jobs_sorted = sorted(
            self.job_list,
            key=lambda x: (float('inf') if x.get('deadline') is None else int(x['deadline']))
        )

        for job in jobs_sorted:
            job_id = job['id']
            deadline = job.get('deadline')
            scheduled = False

            # Try all routes in round-robin order
            for _ in range(len(self.routes)):
                route_key = self._get_next_route_key()
                slot_allocations = self._find_available_slots(job_id, route_key, deadline)

                if not slot_allocations:
                    continue

                metrics = self._get_job_metrics(job_id, route_key)

                # Allocate time to each slot
                for slot_idx, alloc_fraction, alloc_seconds in slot_allocations:
                    slot_time = self.time_slots[slot_idx]
                    self.capacity[route_key][slot_time] -= alloc_seconds

                    schedule.append({
                        'job_id': job_id,
                        'route': route_key,
                        'source_node': metrics['source_node'],
                        'destination_node': metrics['destination_node'],
                        'forecast_id': slot_time,
                        'allocated_fraction': alloc_fraction,
                        'allocated_time': alloc_seconds,
                        'carbon_emissions': metrics['carbon'],
                        'throughput': metrics['throughput'],
                        'transfer_time': metrics['transfer_time'],
                        'deadline': self.job_deadlines[job_id]
                    })

                scheduled = True
                break

            if not scheduled:
                click.secho(f"⚠️ Failed to schedule Job {job_id} (deadline: {deadline})", fg='yellow')

        return pd.DataFrame(schedule)
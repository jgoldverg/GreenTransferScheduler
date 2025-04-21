from typing import List, Dict

import pandas as pd
from itertools import groupby, count

from .output import OutputFormatter


class CarbonAwarePlanner:
    def __init__(self, associations_df: pd.DataFrame, jobs: List[Dict], nodes: List[Dict],
                 mode: str = 'min'):  # 'min' or 'max'
        self.df = associations_df
        self.mode = mode
        self.job_list = jobs
        self.node_list = nodes
        self.reverse_sort = (mode == 'max')  # True for worst-case
        self.jobs = sorted(jobs,
                           key=lambda x: self._get_job_emissions(x['id']),
                           reverse=self.reverse_sort)
        self.capacity = {n['name']: {s: 3600 for s in associations_df['forecast_id'].unique()}
                         for n in nodes}

        self.time_slots = sorted(associations_df['forecast_id'].unique())

        self.output_formatter = OutputFormatter(
            associations_df=self.df,
            job_list=self.job_list,
            node_list=self.node_list,
            time_slots=self.time_slots
        )

    def _get_job_emissions(self, job_id: str) -> float:
        """Get min/max possible emissions for a job based on mode"""
        job_emissions = self.df[self.df['job_id'] == job_id]['carbon_emissions']
        return job_emissions.max() if self.reverse_sort else job_emissions.min()

    def plan(self) -> pd.DataFrame:
        schedule = []
        for idx, job in enumerate(self.jobs):
            self._allocate_job(idx, job, schedule)
        schedule_df = pd.DataFrame(schedule)

        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename=f'carbon_aware_{self.mode}_case.csv',
            optimization_mode=f'greedy_{self.mode}'
        )

    def _allocate_job(self, idx: int, job: Dict, schedule: List):
        job_df = self.df[self.df['job_id'] == job['id']]

        # Pick node with min/max average emissions
        node_emissions = job_df.groupby('node')['carbon_emissions'].mean()
        target_node = node_emissions.idxmax() if self.reverse_sort else node_emissions.idxmin()

        # Sort slots by emissions (descending for max, ascending for min)
        slots_sorted = job_df[job_df['node'] == target_node].sort_values(
            'carbon_emissions', ascending=not self.reverse_sort)

        time_needed = slots_sorted['transfer_time'].iloc[0]
        allocated = False

        # Try single-slot allocation
        for _, slot in slots_sorted.iterrows():
            if self.capacity[target_node][slot['forecast_id']] >= time_needed:
                self._add_entry(idx, job, target_node, slot, time_needed, schedule)
                allocated = True
                break

        # Multi-slot fallback
        if not allocated and time_needed > 3600:
            self._allocate_multislot(idx, job, target_node, slots_sorted, time_needed, schedule)

    def _allocate_multislot(self, idx: int, job: Dict, node: str, slots: pd.DataFrame,
                            time_needed: float, schedule: List):
        consecutive_slots = self._find_consecutive(slots['forecast_id'].unique())
        for seq in consecutive_slots:
            total_capacity = sum(self.capacity[node][s] for s in seq)
            if total_capacity >= time_needed:
                remaining = time_needed
                for slot_id in seq:
                    alloc = min(remaining, self.capacity[node][slot_id])
                    if alloc > 0:
                        slot = slots[slots['forecast_id'] == slot_id].iloc[0]
                        self._add_entry(idx, job, node, slot, alloc, schedule)
                        remaining -= alloc
                return

    def _find_consecutive(self, slots: List[int]) -> List[List[int]]:
        return [list(g) for _, g in groupby(sorted(slots), key=lambda n, c=count(): n - next(c))]

    def _add_entry(self, idx: int, job: Dict, node: str, slot: pd.Series, alloc: float, schedule: List):
        self.capacity[node][slot['forecast_id']] -= alloc
        schedule.append({
            'idx': idx,
            'job_id': job['id'],
            'node': node,
            'forecast_id': slot['forecast_id'],
            'allocated_time': alloc,
            'carbon_emissions': slot['carbon_emissions'],
            'throughput': slot['throughput'],
            'transfer_time': slot['transfer_time'],
            'deadline': job.get('deadline'),
            'extendable': job.get('extendable', False)
        })

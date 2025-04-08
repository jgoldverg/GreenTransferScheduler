import pandas as pd
from .output import OutputFormatter
import click


class WorstCasePlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.slot_duration = 3600  # seconds per time slot

        # Initialize OutputFormatter
        self.output_formatter = OutputFormatter(
            associations_df=self.associations_df,
            job_list=job_list,
            node_list=node_list,
            time_slots=self.time_slots
        )

        # Track remaining capacity per node per slot
        self.remaining_capacity = {
            node['name']: {slot: self.slot_duration for slot in self.time_slots}
            for node in node_list
        }

    def plan(self):
        """Generate the worst-case carbon emissions schedule with continuous time allocation."""
        schedule = []
        unallocated_jobs = []

        # Sort jobs by carbon intensity potential (highest possible emissions first)
        sorted_jobs = sorted(self.job_list,
                             key=lambda x: self._get_max_possible_emissions(x['id']),
                             reverse=True)

        for job in sorted_jobs:
            job_id = job['id']
            job_df = self.associations_df[self.associations_df['job_id'] == job_id]

            # Get the node with highest average carbon emissions for this job
            best_node = job_df.groupby('node')['carbon_emissions'].mean().idxmax()
            node_df = job_df[job_df['node'] == best_node]

            # Sort slots by carbon emissions (highest first)
            sorted_slots = node_df.sort_values('carbon_emissions', ascending=False)

            allocated = self._allocate_job_continuous(
                job_id,
                best_node,
                sorted_slots,
                schedule
            )

            if not allocated:
                unallocated_jobs.append(job_id)
                click.secho(f"Warning: Could not allocate job {job_id}", fg='yellow')

        if unallocated_jobs:
            click.secho(f"\nWarning: {len(unallocated_jobs)} jobs could not be allocated:", fg='red')
            for job_id in unallocated_jobs:
                click.secho(f"  - Job {job_id}", fg='red')

        schedule_df = pd.DataFrame(schedule)
        click.secho(f"Schedule Df: {schedule_df}")
        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename='worst_case_schedule.csv',
            optimization_mode='worst-case'
        )

    def _get_max_possible_emissions(self, job_id):
        """Helper to get maximum possible emissions for a job across all options"""
        job_df = self.associations_df[self.associations_df['job_id'] == job_id]
        return job_df['carbon_emissions'].max()

    def _allocate_job_continuous(self, job_id, node, sorted_slots, schedule):
        """Attempt to allocate job across continuous slots if needed"""
        job_df = sorted_slots[sorted_slots['job_id'] == job_id]
        required_time = job_df.iloc[0]['transfer_time']

        # Try to find a single slot with enough capacity
        for _, slot_row in sorted_slots.iterrows():
            slot = slot_row['forecast_id']
            if self.remaining_capacity[node][slot] >= required_time:
                self._allocate_to_slot(job_id, node, slot, required_time, slot_row, schedule)
                return True

        # If no single slot has enough capacity, try consecutive slots
        if required_time > self.slot_duration:
            return self._allocate_across_multiple_slots(job_id, node, sorted_slots, required_time, schedule)

        return False

    def _allocate_across_multiple_slots(self, job_id, node, sorted_slots, required_time, schedule):
        """Allocate job across multiple consecutive slots"""
        # Find the highest-carbon sequence of consecutive slots
        slots = sorted_slots['forecast_id'].unique()

        # Group slots into consecutive sequences
        consecutive_sequences = []
        current_sequence = []

        for slot in sorted(slots):
            if not current_sequence or slot == current_sequence[-1] + 1:
                current_sequence.append(slot)
            else:
                if len(current_sequence) > 0:
                    consecutive_sequences.append(current_sequence)
                current_sequence = [slot]

        if current_sequence:
            consecutive_sequences.append(current_sequence)

        # Try to find a sequence with enough total capacity
        for sequence in consecutive_sequences:
            total_available = sum(self.remaining_capacity[node][s] for s in sequence)
            if total_available >= required_time:
                remaining_to_allocate = required_time

                for slot in sequence:
                    if remaining_to_allocate <= 0:
                        break

                    alloc_in_slot = min(remaining_to_allocate, self.remaining_capacity[node][slot])
                    if alloc_in_slot > 0:
                        slot_row = sorted_slots[sorted_slots['forecast_id'] == slot].iloc[0]
                        self._allocate_to_slot(
                            job_id,
                            node,
                            slot,
                            alloc_in_slot,
                            slot_row,
                            schedule,
                            partial=True
                        )
                        remaining_to_allocate -= alloc_in_slot

                return True

        return False

    def _allocate_to_slot(self, job_id, node, slot, alloc_time, slot_row, schedule, partial=False):
        """Allocate job to a specific slot"""
        self.remaining_capacity[node][slot] -= alloc_time

        schedule.append({
            'job_id': job_id,
            'node': node,
            'forecast_id': slot,
            'transfer_time': alloc_time,
            'carbon_emissions': slot_row['carbon_emissions'],
            'throughput': slot_row['throughput'],
            'is_partial': partial
        })
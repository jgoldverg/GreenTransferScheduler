import pandas as pd
from datetime import datetime
from .output import OutputFormatter


class BasicPlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list  # No initial sorting - pure round-robin
        self.slot_capacity = 3600  # 1-hour slots
        self.max_slots = 24  # 24-hour window
        self.time_slots = sorted(associations_df['forecast_id'].unique())[:self.max_slots]
        self.remaining_capacity = {slot: self.slot_capacity for slot in self.time_slots}

        # Initialize OutputFormatter
        self.output_formatter = OutputFormatter(
            associations_df=self.associations_df,
            job_list=job_list,
            node_list=node_list,
            time_slots=self.time_slots
        )

        # Precompute metrics
        self.metrics = {
            (int(row.job_id), int(row.forecast_id), row.node): {
                'carbon': row.carbon_emissions,
                'throughput': row.throughput,
                'transfer_time': row.transfer_time
            }
            for _, row in associations_df.iterrows()
        }

    def get_transfer_time(self, job_id):
        """Retrieve transfer_time from precomputed metrics"""
        matching_keys = [k for k in self.metrics.keys() if k[0] == job_id]
        if matching_keys:
            return self.metrics[matching_keys[0]]['transfer_time']
        return self.associations_df[self.associations_df['job_id'] == job_id]['transfer_time'].values[0]

    def plan(self):
        schedule = []
        current_slot_index = 0
        node_index = 0
        unscheduled_jobs = []

        for job in self.job_list:
            job_id = job['id']
            job_deadline = datetime.fromisoformat(job['deadline'])
            job_extendable = job['extendable']
            job_duration = self.get_transfer_time(job_id)
            remaining_job_duration = job_duration
            selected_node = self.node_list[node_index]['name']
            node_index = (node_index + 1) % len(self.node_list)

            scheduled = False

            while remaining_job_duration > 0:
                if current_slot_index >= len(self.time_slots):
                    if job_extendable:
                        # Try to find any remaining capacity in earlier slots
                        for slot in self.time_slots:
                            if self.remaining_capacity[slot] > 0:
                                allocated_time = min(remaining_job_duration, self.remaining_capacity[slot])
                                # ... (same allocation logic as below)
                                break
                        else:
                            unscheduled_jobs.append(job_id)
                            break
                    else:
                        unscheduled_jobs.append(job_id)
                        break

                current_slot = self.time_slots[current_slot_index]
                slot_time = datetime.fromtimestamp(current_slot)  # Convert slot to datetime

                # Deadline check for non-extendable jobs
                if not job_extendable and slot_time > job_deadline:
                    unscheduled_jobs.append(job_id)
                    break

                allocated_time = min(remaining_job_duration, self.remaining_capacity[current_slot])

                # Record allocation
                schedule.append({
                    'idx': len(schedule),
                    'job_id': job_id,
                    'node': selected_node,
                    'forecast_id': current_slot,
                    'allocated_time': allocated_time,
                    'carbon_emissions': self.metrics.get((job_id, current_slot, selected_node), {}).get('carbon', 0),
                    'throughput': self.metrics.get((job_id, current_slot, selected_node), {}).get('throughput', 0),
                    'transfer_time': self.metrics.get((job_id, current_slot, selected_node), {}).get('transfer_time',
                                                                                                     0),
                    'deadline': job['deadline'],
                    'extendable': job_extendable
                })

                remaining_job_duration -= allocated_time
                self.remaining_capacity[current_slot] -= allocated_time

                if self.remaining_capacity[current_slot] == 0:
                    current_slot_index += 1

                scheduled = True

        schedule_df = pd.DataFrame(schedule)

        # Print warning about unscheduled jobs
        if unscheduled_jobs:
            print(f"Warning: Could not schedule {len(unscheduled_jobs)} jobs (IDs: {unscheduled_jobs})")

        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename='basic_planner.csv',
            optimization_mode='basic'
        )
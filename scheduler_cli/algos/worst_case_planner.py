import pandas as pd
from .output import OutputFormatter
import click


class WorstCasePlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())

        # Initialize OutputFormatter
        self.output_formatter = OutputFormatter(
            associations_df=self.associations_df,
            job_list=job_list,
            node_list=node_list,
            time_slots=self.time_slots
        )

        # Track remaining capacity (3600 seconds per slot)
        self.remaining_capacity = {
            node['name']: {slot: 3600 for slot in self.time_slots}
            for node in node_list
        }



    def plan(self):
        """Generate the worst-case carbon emissions schedule."""
        schedule = []
        unallocated_jobs = []

        for job in self.job_list:
            job_id = job['id']
            job_df = self.associations_df[self.associations_df['job_id'] == job_id]
            # job_length =
            highest_ce_slots = job_df.sort_values(by=['carbon_emissions'], ascending=False)
            print(highest_ce_slots)



        schedule_df = pd.DataFrame(schedule)

        return self.output_formatter.format_output(
            schedule_df=schedule_df,
            filename='worst_case_schedule.csv',
            optimization_mode='worst-case'
        )
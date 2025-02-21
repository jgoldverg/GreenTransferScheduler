import math

import numpy as np
import pandas as pd
from enum import Enum
import click
from simgrid_simulator import SimGridSimulator
import pulp


class PlanAlgorithm(Enum):
    RANDOM = "random"
    WORST_CASE = "worst"
    BRUTE_FORCE_GREEN_CASE = "green"
    LINEAR_PROGRAMMING_GREEN = "lp_green"


class BruteForceGreenPlanner:
    def __init__(self, simgrid_simulator, associations_df, node_list):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = sorted(
            associations_df[['job_id', 'transfer_time']].drop_duplicates().to_dict('records'),
            key=lambda x: x['transfer_time']
        )  # Sort jobs by transfer_time (shortest first)
        self.columns = self.associations_df['node'].unique()
        self.rows = self.associations_df['forecast_id'].unique()
        self.remaining_capacity = {node: {slot: 3600 for slot in self.rows} for node in
                                   self.columns}  # Track available seconds per slot
        self.simulator = simgrid_simulator

    def find_best_slots(self, node_name, job_id, job_duration):
        """Finds multiple non-contiguous slots with the lowest CI to fit the job."""
        node_forecasts = self.associations_df[self.associations_df['node'] == node_name]
        available_slots = []

        for slot in self.rows:
            if self.remaining_capacity[node_name][slot] >= min(3600, job_duration):
                ci = node_forecasts[node_forecasts['forecast_id'] == slot]['avg_ci'].mean()
                available_slots.append((slot, ci))

        # Sort by CI to prioritize cleaner slots
        available_slots.sort(key=lambda x: x[1])

        allocated_slots = []
        total_allocated_time = 0

        for slot, _ in available_slots:
            slot_time = min(3600, job_duration - total_allocated_time)
            allocated_slots.append(slot)
            total_allocated_time += slot_time
            self.remaining_capacity[node_name][slot] -= slot_time

            if total_allocated_time >= job_duration:
                break

        return allocated_slots if total_allocated_time >= job_duration else None

    def plan(self):
        schedule = []  # List to store the schedule

        for job in self.job_list:
            best_node = None
            best_slot = None
            best_avg_ci = float('inf')
            job_duration = job['transfer_time']

            for node in self.columns:
                slot_range = self.find_best_slots(node, job['job_id'], job_duration)
                if slot_range is not None:
                    avg_ci = self.associations_df[
                        (self.associations_df['node'] == node) &
                        (self.associations_df['forecast_id'].isin(slot_range))
                        ]['avg_ci'].mean()

                    if avg_ci < best_avg_ci:
                        best_avg_ci = avg_ci
                        best_node = node
                        best_slot = slot_range

            if best_slot is not None:
                for slot in best_slot:
                    # Calculate allocated time for this slot
                    allocated_time = min(3600, job_duration)
                    job_duration -= allocated_time

                    # Look up carbon emissions for this allocation
                    carbon_emissions = self.associations_df[
                        (self.associations_df['job_id'] == job['job_id']) &
                        (self.associations_df['node'] == best_node) &
                        (self.associations_df['forecast_id'] == slot)
                    ]['carbon_emissions'].values[0]

                    # Add the allocation to the schedule
                    schedule.append({
                        'job_id': job['job_id'],
                        'node': best_node,
                        'forecast_id': slot,
                        'allocated_time': allocated_time,
                        'carbon_emissions': carbon_emissions
                    })

        # Convert the schedule to a DataFrame
        schedule_df = pd.DataFrame(schedule)
        schedule_df.reset_index(inplace=True)  # Add idx column
        schedule_df.rename(columns={'index': 'idx'}, inplace=True)

        # Save and return the schedule
        schedule_df.to_csv('../schedules/green_schedule.csv', index=False)
        return schedule_df


import pulp
import pandas as pd
import click


class LinearGreenPlanner:
    def __init__(self, simgrid_simulator, associations_df, node_list):
        self.associations_df = associations_df
        self.node_list = [node['name'] for node in node_list]
        self.job_list = list(associations_df['job_id'].astype(int).unique())
        self.job_list = [int(j) for j in self.job_list]
        self.time_slots = associations_df['forecast_id'].unique()
        self.time_slots = list(map(int, self.time_slots))

        self.problem = pulp.LpProblem("Green_Schedule_Optimization", pulp.LpMinimize)

        # Binary decision variables: x[j, t, n] = 1 if job j is scheduled in time slot t on node n
        self.x = pulp.LpVariable.dicts("x",
                                       [(j, t, n) for j in self.job_list for t in self.time_slots for n in
                                        self.node_list],
                                       cat='Binary')

        # Carbon emissions for each job, time slot, and node combination
        self.emissions = {(row.job_id, row.forecast_id, row.node): row.carbon_emissions
                          for _, row in associations_df.iterrows()}

    def plan(self):
        # Objective: Minimize total carbon emissions
        self.problem += pulp.lpSum(
            self.x[j, t, n] * self.emissions.get((j, t, n), 0)
            for j in self.job_list for t in self.time_slots for n in self.node_list
        )

        # Constraints
        # 1. Each job must be fully scheduled
        for j in self.job_list:
            job_duration = self.associations_df[self.associations_df['job_id'] == j]['transfer_time'].values[0]
            self.problem += pulp.lpSum(
                self.x[j, t, n] * 3600 for t in self.time_slots for n in self.node_list
            ) >= job_duration

        # 2. No time slot can exceed its capacity (3600 seconds)
        for t in self.time_slots:
            for n in self.node_list:
                self.problem += pulp.lpSum(
                    self.x[j, t, n] * 3600 for j in self.job_list
                ) <= 3600

        # Solve the problem
        self.problem.solve()

        # Check solver status and print colored output
        status = pulp.LpStatus[self.problem.status]
        if status == "Optimal" or status == "Feasible":
            click.secho(f"Solver status: {status} (Feasible solution found)", fg='green')
        else:
            click.secho(f"Solver status: {status} (No feasible solution found)", fg='red')

        # Extract results
        schedule = []
        for j in self.job_list:
            for t in self.time_slots:
                for n in self.node_list:
                    if pulp.value(self.x[j, t, n]) == 1:  # Job j is scheduled in time slot t on node n
                        carbon_emissions = self.emissions.get((j, t, n), 0)
                        schedule.append({
                            'job_id': j,
                            'node': n,
                            'forecast_id': t,
                            'carbon_emissions': carbon_emissions
                        })

        # Convert to DataFrame
        schedule_df = pd.DataFrame(schedule)
        schedule_df.reset_index(inplace=True)  # Add idx column
        schedule_df.rename(columns={'index': 'idx'}, inplace=True)

        # Save and return the schedule
        schedule_df.to_csv('/workspace/schedules/linear_programming_green.csv', index=False)
        return schedule_df


class RandomPlanner:
    def plan(self):
        pass


class WorstCasePlanner:
    def plan(self):
        pass


# Factory function to instantiate correct class
def planner_factory(algo: PlanAlgorithm, *args, **kwargs):
    planners = {
        PlanAlgorithm.RANDOM: RandomPlanner,
        PlanAlgorithm.WORST_CASE: WorstCasePlanner,
        PlanAlgorithm.BRUTE_FORCE_GREEN_CASE: BruteForceGreenPlanner,
        PlanAlgorithm.LINEAR_PROGRAMMING_GREEN: LinearGreenPlanner
    }
    return planners[PlanAlgorithm(algo)](*args, **kwargs)  # Instantiate the selected planner

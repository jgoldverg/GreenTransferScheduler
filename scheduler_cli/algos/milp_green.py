import pulp
import pandas as pd
import click
import math
from .output import OutputFormatter


class MixedIntegerLinearProgrammingGreenPlanner:
    def __init__(self, associations_df, job_list, node_list, optimize_mode='both'):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.optimize_mode = optimize_mode.lower()
        self.max_slot = max(self.time_slots)

        # Initialize output formatter
        self.output_formatter = OutputFormatter(job_list, node_list, self.time_slots, self.associations_df)

        # Precompute metrics
        self.metrics = {
            (int(row.job_id), int(row.forecast_id), row.node): {
                'time': row.transfer_time,
                'carbon': row.carbon_emissions,
                'throughput': row.throughput,
                'per_slot_time': min(3600, row.transfer_time)
            }
            for _, row in associations_df.iterrows()
        }

        # Calculate job requirements (assuming each job has 'required_time' or use transfer_time)
        self.job_requirements = {
            j['id']: max(
                row.transfer_time
                for _, row in associations_df[associations_df['job_id'] == j['id']].iterrows()
            )
            for j in job_list
        }

        # Initialize problem
        self.problem = pulp.LpProblem("Green_Job_Scheduler", pulp.LpMinimize)

        # Decision variables: x[j,t,n] = fraction of job j allocated to slot t on node n
        self.x = pulp.LpVariable.dicts(
            "allocation",
            [(j['id'], t, n['name']) for j in self.job_list
             for t in self.time_slots
             for n in self.node_list],
            lowBound=0, upBound=1, cat='Continuous'
        )

    def plan(self):
        # Objective function
        if self.optimize_mode == 'time':
            obj = pulp.lpSum(
                self.x[j['id'], t, n['name']] * self.metrics[(j['id'], t, n['name'])]['carbon']
                for j in self.job_list
                for t in self.time_slots
                for n in self.node_list
            )
        elif self.optimize_mode == 'space':
            obj = pulp.lpSum(
                self.x[j['id'], t, n['name']] * (1 / self.metrics[(j['id'], t, n['name'])]['throughput'])
                for j in self.job_list
                for t in self.time_slots
                for n in self.node_list
            )
        else:  # both
            obj = pulp.lpSum(
                self.x[j['id'], t, n['name']] * (
                        0.7 * self.metrics[(j['id'], t, n['name'])]['carbon'] +
                        0.3 * (1 / self.metrics[(j['id'], t, n['name'])]['throughput'])
                )
                for j in self.job_list
                for t in self.time_slots
                for n in self.node_list
            )
        self.problem += obj

        ### Constraints ###

        # 1. Each job must complete its required time
        for j in self.job_list:
            self.problem += pulp.lpSum(
                self.x[j['id'], t, n['name']] * self.metrics[(j['id'], t, n['name'])]['per_slot_time']
                for t in self.time_slots
                for n in self.node_list
            ) >= self.job_requirements[j['id']]

        # 2. Node capacity constraint (knapsack-like)
        for t in self.time_slots:
            for n in self.node_list:
                self.problem += pulp.lpSum(
                    self.x[j['id'], t, n['name']] * self.metrics[(j['id'], t, n['name'])]['per_slot_time']
                    for j in self.job_list
                ) <= 3600  # 1 hour capacity

        # 3. Optional: Limit migrations (can be commented out for full flexibility)
        # self._add_migration_constraints()

        # Solve with longer time limit
        self.problem.solve(pulp.PULP_CBC_CMD(msg=True, timeLimit=5000))
        status = pulp.LpStatus[self.problem.status]

        if status != "Optimal":
            click.secho(f"Solver status: {status}", fg='red')
            return None

        return self._generate_migratable_schedule(status)

    def _add_migration_constraints(self):
        """Optional constraints to limit excessive migrations"""
        # Binary variables indicating if job j uses node n at all
        self.y = pulp.LpVariable.dicts(
            "node_usage",
            [(j['id'], n['name']) for j in self.job_list for n in self.node_list],
            cat='Binary'
        )

        # Link x and y variables
        for j in self.job_list:
            for n in self.node_list:
                self.problem += pulp.lpSum(
                    self.x[j['id'], t, n['name']] for t in self.time_slots
                ) <= self.y[j['id'], n['name']] * len(self.time_slots)

        # Limit number of nodes per job (e.g., max 2 nodes)
        for j in self.job_list:
            self.problem += pulp.lpSum(
                self.y[j['id'], n['name']] for n in self.node_list
            ) <= 2

    def _generate_migratable_schedule(self, status):
        schedule = []
        job_remaining = {j['id']: self.job_requirements[j['id']] for j in self.job_list}

        for t in sorted(self.time_slots):
            for j in self.job_list:
                for n in self.node_list:
                    alloc = pulp.value(self.x[j['id'], t, n['name']])
                    if alloc > 0.01 and job_remaining[j['id']] > 0:
                        time_alloc = min(
                            alloc * self.metrics[(j['id'], t, n['name'])]['per_slot_time'],
                            job_remaining[j['id']]
                        )

                        if time_alloc > 0:
                            schedule.append({
                                'job_id': j['id'],
                                'node': n['name'],
                                'forecast_id': t,
                                'allocated_time': time_alloc,
                                'carbon_emissions': time_alloc * (
                                            self.metrics[(j['id'], t, n['name'])]['carbon'] / 3600),
                                'throughput': self.metrics[(j['id'], t, n['name'])]['throughput']
                            })
                            job_remaining[j['id']] -= time_alloc

        schedule_df = pd.DataFrame(schedule)
        return self.output_formatter.format_output(
            schedule_df,
            filename='milp_green.csv',
            optimization_mode=f'Milp {self.optimize_mode}'
        )
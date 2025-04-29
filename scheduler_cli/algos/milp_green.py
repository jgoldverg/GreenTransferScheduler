import click
import pandas as pd
import pulp


class MixedIntegerLinearProgrammingGreenPlanner:
    def __init__(self, associations_df, job_list):
        self.associations_df = associations_df
        self.node_list = self.associations_df['node'].unique()
        click.secho(f"Node list {self.node_list}")
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.max_slot = max(self.time_slots)

        # Store deadlines for each job
        self.job_deadlines = {
            j['id']: j.get('deadline', self.max_slot)  # Default to max slot if no deadline
            for j in job_list
        }

        # Initialize output formatter

        # Precompute metrics
        self.metrics = {
            (int(row.job_id), int(row.forecast_id), row.node): {
                'time': row.transfer_time,
                'carbon': row.carbon_emissions,
                'throughput': row.throughput,
                'per_slot_time': min(3600, row.transfer_time),
            }
            for _, row in associations_df.iterrows()
        }

        # Calculate job requirements
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
        # Only create variables for time slots before the deadline
        self.x = pulp.LpVariable.dicts(
            "allocation",
            [(j['id'], t, n) for j in self.job_list
             for t in self.time_slots if t <= self.job_deadlines[j['id']]
             for n in self.node_list],
            lowBound=0, upBound=1, cat='Continuous'
        )

        # Slack variables: unmet job time
        self.unmet = pulp.LpVariable.dicts(
            "unmet_time",
            [j['id'] for j in self.job_list],
            lowBound=0,
            cat='Continuous'
        )

    def plan(self):
        # Penalty for unmet job time (adjustable)
        PENALTY_PER_SECOND = 1000

        # Objective function: minimize carbon emissions + penalty for unmet work
        self.problem += (
                pulp.lpSum(
                    self.x[j['id'], t, n] * self.metrics[(j['id'], t, n)]['carbon']
                    for j in self.job_list
                    for t in self.time_slots if t <= self.job_deadlines[j['id']]
                    for n in self.node_list
                ) +
                pulp.lpSum(
                    self.unmet[j['id']] * PENALTY_PER_SECOND
                    for j in self.job_list
                )
        )

        # 1. Job completion (relaxed with unmet_time)
        for j in self.job_list:
            self.problem += pulp.lpSum(
                self.x[j['id'], t, n] * self.metrics[(j['id'], t, n)]['per_slot_time']
                for t in self.time_slots if t <= self.job_deadlines[j['id']]
                for n in self.node_list
            ) + self.unmet[j['id']] >= self.job_requirements[j['id']]

        # 2. Node capacity constraint
        for t in self.time_slots:
            for n in self.node_list:
                self.problem += pulp.lpSum(
                    self.x[j['id'], t, n] * self.metrics[(j['id'], t, n)]['per_slot_time']
                    for j in self.job_list if t <= self.job_deadlines[j['id']]
                ) <= 3600  # 1 hour

        # Solve with time limit
        self.problem.solve(pulp.PULP_CBC_CMD(msg=True, timeLimit=5000))
        status = pulp.LpStatus[self.problem.status]

        if status not in ["Optimal", "Feasible"]:
            click.secho(f"Solver status: {status}", fg='red')
            return None

        return self._generate_migratable_schedule(status)

    def _generate_migratable_schedule(self, status):
        schedule = []
        job_remaining = {j['id']: self.job_requirements[j['id']] for j in self.job_list}
        job_dict = {j['id']: j for j in self.job_list}

        for t in sorted(self.time_slots):
            for j in self.job_list:
                job_id = j['id']
                # Skip time slots after deadline
                if t > self.job_deadlines[job_id]:
                    continue

                for n in self.node_list:
                    # Skip if variable doesn't exist (for slots after deadline)
                    if (j['id'], t, n) not in self.x:
                        continue

                    alloc = pulp.value(self.x[j['id'], t, n])
                    if alloc > 0.01 and job_remaining[j['id']] > 0:
                        per_slot_time = self.metrics[(j['id'], t, n)]['per_slot_time']
                        time_alloc = min(alloc * per_slot_time, job_remaining[j['id']])
                        if time_alloc > 0:
                            slot_data = self.metrics[(j['id'], t, n)]
                            schedule.append({
                                'job_id': job_id,
                                'node': n,
                                'forecast_id': t,
                                'allocated_time': time_alloc,
                                'carbon_emissions': time_alloc * (slot_data['carbon'] / 3600),
                                'bytes': slot_data.get('bytes'),
                                'throughput': slot_data.get('throughput'),
                                'transfer_time': slot_data.get('time'),
                                'deadline': self.job_deadlines[job_id]
                            })
                            job_remaining[j['id']] -= time_alloc

        return pd.DataFrame(schedule)
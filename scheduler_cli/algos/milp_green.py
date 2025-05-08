import click
import pandas as pd
import pulp


class MixedIntegerLinearProgrammingGreenPlanner:
    def __init__(self, associations_df, job_list):
        self.associations_df = associations_df
        self.route_list = self.associations_df['route_key'].unique()
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.max_slot = max(self.time_slots)

        # Store deadlines for each job
        self.job_deadlines = {
            j['id']: j.get('deadline', self.max_slot)
            for j in job_list
        }

        # Precompute metrics - each entry represents a complete job option
        self.metrics = {}
        for _, row in associations_df.iterrows():
            key = (int(row.job_id), int(row.forecast_id), row.route_key)
            self.metrics[key] = {
                'transfer_time': row.transfer_time,
                'carbon': row.carbon_emissions,
                'source_node': row.source_node,
                'destination_node': row.destination_node,
                'throughput': row.throughput
            }

        # Initialize problem
        self.problem = pulp.LpProblem("Carbon_Minimizing_Scheduler", pulp.LpMinimize)

        # Decision variables: x[j,t,r] = fraction of job j allocated to slot t on route r
        # (1 = entire job, 0.5 = half the job, etc.)
        self.x = pulp.LpVariable.dicts(
            "allocation",
            [(j['id'], t, r) for j in self.job_list
             for t in self.time_slots if t <= self.job_deadlines[j['id']]
             for r in self.route_list if (j['id'], t, r) in self.metrics],
            lowBound=0, upBound=1, cat='Continuous'
        )

    def plan(self):
        # Objective: Minimize total carbon emissions
        self.problem += pulp.lpSum(
            self.x[j, t, r] * self.metrics[(j, t, r)]['carbon']
            for (j, t, r) in self.x
        )

        # Constraints: each job must be fully allocated (sum of fractions = 1)
        for j in self.job_list:
            job_id = j['id']
            deadline = self.job_deadlines[job_id]

            self.problem += pulp.lpSum(
                self.x[job_id, t, r]
                for t in self.time_slots if t <= deadline
                for r in self.route_list
                if (job_id, t, r) in self.x
            ) == 1  # Must allocate 100% of each job

        # Route capacity constraints: sum of (fraction * transfer_time) <= 3600 per slot/route
        for t in self.time_slots:
            for r in self.route_list:
                self.problem += pulp.lpSum(
                    self.x[j['id'], t, r] * self.metrics[(j['id'], t, r)]['transfer_time']
                    for j in self.job_list
                    if t <= self.job_deadlines[j['id']] and (j['id'], t, r) in self.x
                ) <= 3600

        # Solve
        self.problem.solve(pulp.PULP_CBC_CMD(msg=True, timeLimit=5000))
        status = pulp.LpStatus[self.problem.status]

        if status not in ["Optimal", "Feasible"]:
            click.secho(f"Solver status: {status}", fg='red')
            return None

        return self._generate_schedule(status)

    def _generate_schedule(self, status):
        schedule = []

        for (j, t, r), var in self.x.items():
            alloc = pulp.value(var)
            if alloc > 0.001:  # Only include meaningful allocations
                metrics = self.metrics[(j, t, r)]
                schedule.append({
                    'job_id': j,
                    'route': r,
                    'source_node': metrics['source_node'],
                    'destination_node': metrics['destination_node'],
                    'forecast_id': t,
                    'allocated_fraction': alloc,
                    'allocated_time': alloc * metrics['transfer_time'],
                    'carbon_emissions': alloc * metrics['carbon'],
                    'throughput': metrics['throughput'],
                    'transfer_time': metrics['transfer_time'],
                    'deadline': self.job_deadlines[j]
                })

        return pd.DataFrame(schedule)
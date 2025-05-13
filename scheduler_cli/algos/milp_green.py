import pulp
import pandas as pd


class LexicographicGreenPlanner:
    def __init__(self, associations_df, job_list):
        self.df = associations_df
        self.jobs = {j['id']: j for j in job_list}
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.routes = associations_df['route_key'].unique()
        self.max_slot = max(self.time_slots)

        # Precompute valid combinations
        self.valid = []
        self.carbon = {}
        self.throughput = {}
        self.transfer_time = {}

        for j in job_list:
            job_id = j['id']
            deadline = j.get('deadline', self.max_slot)
            for t in self.time_slots:
                if t > deadline:
                    continue
                for r in self.routes:
                    mask = (
                            (self.df['job_id'] == job_id) &
                            (self.df['forecast_id'] == t) &
                            (self.df['route_key'] == r)
                    )
                    if not self.df[mask].empty:
                        data = self.df[mask].iloc[0]
                        key = (job_id, t, r)
                        self.valid.append(key)
                        self.carbon[key] = float(data['carbon_emissions'])
                        self.throughput[key] = float(data['throughput']) / 8  # bytes/sec
                        self.transfer_time[key] = float(data['transfer_time'])

        # Initialize models
        self.job_model = pulp.LpProblem("MaxJobs_Stage", pulp.LpMaximize)
        self.full_model = pulp.LpProblem("MinCarbon_Stage", pulp.LpMinimize)

        # Variables
        self.x = pulp.LpVariable.dicts("x", self.valid, 0, 1, pulp.LpContinuous)
        self.y = pulp.LpVariable.dicts("y", self.jobs.keys(), pulp.LpBinary)

    def plan(self):
        # STAGE 1: Maximize number of jobs completed
        self._build_common_constraints()
        self.job_model += pulp.lpSum(self.y[j] for j in self.jobs)
        self.job_model.solve(pulp.PULP_CBC_CMD(msg=False))
        max_jobs = int(pulp.value(self.job_model.objective))

        # STAGE 2: Minimize carbon with job completion constraint
        self._build_common_constraints()
        self.full_model += pulp.lpSum(
            self.x[key] * self.carbon[key] for key in self.valid
        )
        self.full_model += pulp.lpSum(self.y[j] for j in self.jobs) >= max_jobs
        self.full_model.solve(pulp.PULP_CBC_CMD(msg=True, timeLimit=5000))

        return self._generate_schedule()

    def _build_common_constraints(self):
        """Constraints shared by both models"""
        # Job completion constraints
        for j in self.jobs:
            bytes_needed = self.jobs[j]['bytes']
            self.full_model += (
                    pulp.lpSum(3600 * self.throughput[key] * self.x[key]
                               for key in self.valid if key[0] == j) >= bytes_needed * self.y[j]
            )

        # Time slot capacity constraints
        for t in self.time_slots:
            for r in self.routes:
                self.full_model += pulp.lpSum(
                    self.x[key] * self.transfer_time[key]
                    for key in self.valid if key[1] == t and key[2] == r
                ) <= 3600

    def _generate_schedule(self):
        schedule = []
        for key in self.valid:
            j, t, r = key
            x_val = pulp.value(self.x[key])
            if x_val > 1e-6:
                data = self.df[
                    (self.df['job_id'] == j) &
                    (self.df['forecast_id'] == t) &
                    (self.df['route_key'] == r)
                    ].iloc[0]

                schedule.append({
                    'job_id': j,
                    'forecast_id': t,
                    'route_key': r,
                    'allocated_fraction': x_val,
                    'allocated_bytes': x_val * 3600 * self.throughput[key],
                    'carbon_emissions': x_val * self.carbon[key],
                    'completed': pulp.value(self.y[j]) > 0.99
                })

        return pd.DataFrame(schedule)


class MilpGreenPlanner:
    def __init__(self, associations_df, job_list):
        # Keep original for later column access
        self.associations_df_raw = associations_df.copy()

        # Use MultiIndex for efficient access
        self.associations_df = associations_df.set_index(['job_id', 'forecast_id', 'route_key'])
        print(self.associations_df.columns)

        self.route_list = self.associations_df_raw['route_key'].unique()
        self.job_list = job_list
        self.time_slots = sorted(self.associations_df_raw['forecast_id'].unique())
        self.max_slot = max(self.time_slots)

        # Store job deadlines and sizes
        self.job_info = {
            j['id']: {
                'deadline': j.get('deadline', self.max_slot),
                'bytes': j['bytes']
            }
            for j in job_list
        }

        # Precompute metrics for valid combinations
        self.valid_combinations = []
        self.carbon = {}
        self.throughput = {}
        self.transfer_time = {}

        print('processing valid combinations...')
        for j in job_list:
            job_id = j['id']
            deadline = self.job_info[job_id]['deadline']

            for t in self.time_slots:
                if t > deadline:
                    continue

                for r in self.route_list:
                    key = (job_id, t, r)
                    data = self.associations_df.loc[key]
                    self.valid_combinations.append(key)
                    self.carbon[key] = float(data['carbon_emissions'])
                    self.throughput[key] = float(data['throughput']) / 8  # bytes/sec
                    self.transfer_time[key] = float(data['transfer_time'])

        print('finished processing valid combinations')
        # Initialize problem
        self.problem = pulp.LpProblem("MinCarbon_MaxJobs", pulp.LpMinimize)

        # Variables
        self.x = pulp.LpVariable.dicts(
            "x", self.valid_combinations, 0, 1, cat='Continuous'
        )
        self.y = pulp.LpVariable.dicts(
            "y", [j['id'] for j in job_list], cat='Binary'
        )

    def plan(self):
        # Objective: Minimize carbon emissions (primary), maximize jobs completed (secondary)
        max_jobs = len(self.job_list)
        max_carbon = max(self.carbon.values()) * max_jobs if self.carbon else 1

        self.problem += (
                pulp.lpSum(self.x[key] * self.carbon[key] for key in self.valid_combinations) / max_carbon -
                pulp.lpSum(self.y[j['id']] for j in self.job_list) / max_jobs
        )

        # Constraints
        for j in self.job_list:
            job_id = j['id']
            bytes_needed = self.job_info[job_id]['bytes']

            # Job must be fully allocated if completed (y[j] = 1)
            self.problem += (
                    pulp.lpSum(
                        3600 * self.throughput[key] * self.x[key]
                        for key in self.valid_combinations if key[0] == job_id
                    ) >= bytes_needed * self.y[job_id]
            )

            # Cannot allocate to a job if not completed
            for key in self.valid_combinations:
                if key[0] == job_id:
                    self.problem += self.x[key] <= self.y[job_id]

        # Time slot capacity (1 hour = 3600 seconds)
        for t in self.time_slots:
            for r in self.route_list:
                self.problem += (
                        pulp.lpSum(
                            self.x[key] * self.transfer_time[key]
                            for key in self.valid_combinations
                            if key[1] == t and key[2] == r
                        ) <= 3600
                )

        # Solve
        self.problem.solve(pulp.PULP_CBC_CMD(msg=True, timeLimit=5000, threads=4, cuts=True))
        status = pulp.LpStatus[self.problem.status]

        print(f"\nSolver status: {status}")
        print(f"Jobs completed: {sum(pulp.value(self.y[j['id']]) for j in self.job_list)}/{len(self.job_list)}")

        return self._generate_schedule()

    def _generate_schedule(self):
        schedule = []
        for key in self.valid_combinations:
            j, t, r = key
            x_val = pulp.value(self.x[key])
            if x_val > 1e-6:  # Only include non-zero allocations
                data = self.associations_df.loc[(j, t, r)]

                schedule.append({
                    'job_id': j,
                    'forecast_id': t,
                    'route': r,
                    'source_node': data['source_node'],
                    'destination_node': data['destination_node'],
                    'allocated_fraction': x_val,
                    'allocated_time': x_val * 3600,
                    'allocated_bytes': x_val * 3600 * (float(data['throughput']) / 8),
                    'carbon_emissions': x_val * float(data['carbon_emissions']),
                    'throughput': float(data['throughput']),
                    'transfer_time': float(data['transfer_time']),
                    'completed': pulp.value(self.y[j]) > 0.99
                })

        return pd.DataFrame(schedule)

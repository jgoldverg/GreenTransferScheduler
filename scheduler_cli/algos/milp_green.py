import pulp
import pandas as pd
import click


class MixedIntegerLinearProgrammingGreenPlanner:
    def __init__(self, associations_df, job_list, node_list):
        self.associations_df = associations_df
        self.node_list = [node['name'] for node in node_list]  # Extract node names
        self.job_list = [job['id'] for job in job_list]  # Extract job IDs
        self.time_slots = sorted(associations_df['forecast_id'].unique())

        # Initialize the optimization problem
        self.problem = pulp.LpProblem("Green_Schedule_Optimization", pulp.LpMinimize)

        # Binary decision variables: x[j, t, n] = 1 if job j is scheduled in time slot t on node n
        self.x = pulp.LpVariable.dicts("x",
                                       [(j, t, n) for j in self.job_list for t in self.time_slots for n in
                                        self.node_list],
                                       cat='Binary')

        # Carbon emissions for each job, time slot, and node combination
        self.emissions = {
            (int(row.job_id), int(row.forecast_id), row.node): row.carbon_emissions
            for _, row in associations_df.iterrows()
        }

    def get_transfer_time(self, job_id):
        """
        Retrieve the transfer_time for a job from the associations_df.
        """
        transfer_time = self.associations_df[
            self.associations_df['job_id'] == job_id
            ]['transfer_time'].values[0]
        return transfer_time

    def plan(self):
        # Objective: Minimize total carbon emissions
        self.problem += pulp.lpSum(
            self.x[j, t, n] * self.emissions.get((j, t, n), 0)
            for j in self.job_list for t in self.time_slots for n in self.node_list
        )

        # Constraints
        # 1. Each job must be fully scheduled
        for j in self.job_list:
            job_duration = self.get_transfer_time(j)
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

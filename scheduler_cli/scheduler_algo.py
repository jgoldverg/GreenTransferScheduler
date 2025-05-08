import time
from enum import Enum
import click
import pandas as pd

from models import read_in_node_file, read_in_job_file, read_in_node_list_to_map, PlanAlgorithm
from algos.shortest_job_first import ShortestJobFirst
from algos.earliest_deadline_first import EarliestDeadlineFirst

from algos.milp_green import MixedIntegerLinearProgrammingGreenPlanner
from algos.greedy_carbon_planner import CarbonAwarePlanner
from algos.round_robin import RoundRobin
from schedules_visualization import ScheduleVisualization


class Scheduler:

    def __init__(self, node_file_path, job_file_path, df_path):
        # Load in Nodes
        self.node_list = read_in_node_file(node_file_path)
        self.node_map = read_in_node_list_to_map(node_file_path)
        click.secho(f"Loaded {len(self.node_list)} Nodes", fg="green")
        self.job_list = read_in_job_file(job_file_path)
        self.associations_df = pd.read_csv(df_path)
        self.ci_matrix = None
        self.ip_forecast = []

    def create_plan(self, plan_algo: PlanAlgorithm):
        click.secho(f"Running algo: {plan_algo}")

        # Define planner configurations
        planner_configs = {
            PlanAlgorithm.BRUTE_FORCE_GREEN_CASE: {
                'mode': 'min',
            },
            PlanAlgorithm.WORST_CASE: {
                'mode': 'max',
            },
        }
        schedules_map = {}
        algo_time = {}
        if plan_algo == PlanAlgorithm.ALL:
            for plan in PlanAlgorithm:
                if plan == PlanAlgorithm.ALL:
                    continue

                # Get the appropriate kwargs for this planner
                kwargs = planner_configs.get(plan, {})

                planner = planner_factory(
                    plan,
                    self.associations_df,
                    self.job_list,
                    **kwargs
                )
                start_time = time.time()
                schedules_map[plan] = planner.plan()
                total_time = time.time() - start_time
                click.secho(f"Total time used to run {plan}: {total_time}")
                algo_time[str(plan)] = total_time
        else:
            # Get the appropriate kwargs for the specific planner
            kwargs = planner_configs.get(plan_algo, {})

            planner = planner_factory(
                plan_algo,
                self.associations_df,
                self.job_list,
                **kwargs
            )
            start_time = time.time()
            schedules_map[plan_algo] = planner.plan()
            total_time = time.time() - start_time
            algo_time[str(plan_algo)] = total_time
            click.secho(f"Total time used to run {plan_algo}: {total_time}")

        # schedule_map: Dict, associations_df: pd.DataFrame, job_list : List, node_list: List
        self.schedule_visualization = ScheduleVisualization(schedules_map, self.associations_df, self.job_list,
                                                            self.node_list, algo_time)
        self.schedule_visualization.save_schedules()
        self.schedule_visualization.visualize()


# Factory function to instantiate correct class
def planner_factory(algo: PlanAlgorithm, *args, **kwargs):
    planners = {
        PlanAlgorithm.WORST_CASE: CarbonAwarePlanner,
        PlanAlgorithm.BRUTE_FORCE_GREEN_CASE: CarbonAwarePlanner,
        PlanAlgorithm.LINEAR_PROGRAMMING_GREEN: MixedIntegerLinearProgrammingGreenPlanner,
        PlanAlgorithm.ROUND_ROBIN: RoundRobin,
        PlanAlgorithm.SHORTEST_JOB_FIRST: ShortestJobFirst,
        PlanAlgorithm.EARLIEST_DEADLINE_FIRST: EarliestDeadlineFirst,
    }
    return planners[PlanAlgorithm(algo)](*args, **kwargs)  # Instantiate the selected planner

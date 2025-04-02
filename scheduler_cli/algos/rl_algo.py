import numpy as np
import pandas as pd
import gymnasium as gym
from gymnasium import spaces
from typing import Dict

from stable_baselines3 import PPO
from stable_baselines3.common.callbacks import EvalCallback

from .output import OutputFormatter


class JobSchedulingEnv(gym.Env):
    """Custom Environment for multi-objective job scheduling across time"""

    metadata = {'render.modes': ['human']}

    def __init__(self, df: pd.DataFrame,
                 objective: str = 'carbon_emissions',
                 forecast_horizon: int = 24,
                 max_jobs_per_node: int = 5):
        """
        Args:
            df: The associations dataframe
            objective: Optimization target ('carbon_emissions', 'throughput', 'energy')
            forecast_horizon: Number of forecast periods (hours) to consider
            max_jobs_per_node: Maximum jobs allowed per node per time period
        """
        super(JobSchedulingEnv, self).__init__()

        self.df = df
        self.objective = objective
        self.forecast_horizon = forecast_horizon
        self.max_jobs_per_node = max_jobs_per_node

        # Preprocess data
        self._preprocess_data()

        # Define action and observation space
        self.action_space = spaces.Discrete(len(self.nodes))

        self.observation_space = spaces.Dict({
            'job_features': spaces.Box(
                low=np.array([0, 0, 0, 0, 0, 0], dtype=np.float32),
                high=np.array([np.inf, np.inf, np.inf, np.inf, np.inf, np.inf], dtype=np.float32)
            ),
            'current_time': spaces.Discrete(forecast_horizon),
            'node_load': spaces.Box(
                low=np.zeros(len(self.nodes)),
                high=np.full(len(self.nodes), max_jobs_per_node),
                dtype=np.float32
            )
        })

        self.reset()

    def _preprocess_data(self):
        """Prepare the data for environment use"""
        # Ensure forecast_id is within bounds
        self.df = self.df[self.df['forecast_id'] < self.forecast_horizon]

        # Get unique nodes and jobs
        self.nodes = self.df['node'].unique()
        self.jobs = self.df['job_id'].unique()

        # Create mapping for faster lookup
        self.job_data = {
            (job_id, node, forecast): group
            for (job_id, node, forecast), group in self.df.groupby(['job_id', 'node', 'forecast_id'])
        }

        # Sort jobs by some criteria (could be modified)
        self.jobs_sorted = sorted(self.jobs)

    def _get_obs(self) -> Dict[str, np.ndarray]:
        """Get current observation"""
        if self.current_job_idx >= len(self.jobs_sorted):
            return None

        current_job = self.jobs_sorted[self.current_job_idx]

        # Get features for all node options at current time
        job_features = []
        for node in self.nodes:
            key = (current_job, node, self.current_time)
            if key in self.job_data:
                data = self.job_data[key].iloc[0]
                features = [
                    data['throughput'],
                    data['transfer_time'],
                    data['host_joules'],
                    data['link_joules'],
                    data['total_joules'],
                    data['carbon_emissions']
                ]
            else:
                # If no data, use zeros (could also use mean/median)
                features = [0, 0, 0, 0, 0, 0]
            job_features.append(features)

        # Average features across nodes for the observation
        avg_features = np.mean(job_features, axis=0)

        return {
            'job_features': avg_features.astype(np.float32),
            'current_time': self.current_time,
            # 'node_load': np.array([self.node_loads[node] for node in self.nodes], dtype=np.float32)
        }

    def reset(self):
        """Reset the environment state"""
        self.current_job_idx = 0
        self.current_time = 0
        # self.node_loads = {node: 0 for node in self.nodes}
        self.schedule = []
        self.total_reward = 0
        return self._get_obs()

    def step(self, action: int):
        """Execute one action (assign current job to selected node)"""
        if self.current_job_idx >= len(self.jobs_sorted):
            raise ValueError("All jobs have been scheduled")

        current_job = self.jobs_sorted[self.current_job_idx]
        selected_node = self.nodes[action]

        # Get job data at selected node and current time
        key = (current_job, selected_node, self.current_time)

        if key not in self.job_data:
            # Invalid assignment - penalize heavily
            reward = -1000
            done = False
            info = {'status': 'invalid_assignment'}
        else:
            data = self.job_data[key].iloc[0]

            # Calculate reward based on objective
            if self.objective == 'carbon_emissions':
                reward = -data['carbon_emissions']  # Minimize
            elif self.objective == 'throughput':
                reward = data['throughput']  # Maximize
            elif self.objective == 'energy':
                reward = -data['total_joules']  # Minimize
            else:
                raise ValueError(f"Unknown objective: {self.objective}")

            # Penalize for overloading nodes
            # if self.node_loads[selected_node] >= self.max_jobs_per_node:
            #     reward -= 50  # Heavy penalty for overloading

            # Update node load
            # self.node_loads[selected_node] += 1

            # Record the assignment
            self.schedule.append({
                'job_id': current_job,
                'node': selected_node,
                'forecast_id': self.current_time,
                'reward': reward,
                **{k: data[k] for k in ['carbon_emissions', 'throughput', 'total_joules']}
            })

            self.total_reward += reward
            info = {'status': 'assigned', **data.to_dict()}

        # Move to next job
        self.current_job_idx += 1

        # Progress time if we've scheduled all current jobs
        if self.current_job_idx >= len(self.jobs_sorted):
            self.current_time += 1
            self.current_job_idx = 0

            # Reset node loads for new time period
            # self.node_loads = {node: 0 for node in self.nodes}

        # Check if episode is done (scheduled all jobs across all times)
        done = (self.current_time >= self.forecast_horizon or
                (self.current_time == self.forecast_horizon - 1 and
                 self.current_job_idx >= len(self.jobs_sorted)))

        # Get next observation
        obs = self._get_obs()

        return obs, reward, done, info

    def render(self, mode='human'):
        """Render the current schedule"""
        if mode == 'human':
            print(f"\nCurrent Schedule (Time {self.current_time}, Job {self.current_job_idx}/{len(self.jobs_sorted)})")
            print(f"Total Reward: {self.total_reward:.2f}")
            # print("Node Loads:", self.node_loads)

            if len(self.schedule) > 0:
                last_assignment = self.schedule[-1]
                print(f"Last Assignment: Job {last_assignment['job_id']} -> {last_assignment['node']} "
                      f"(Time {last_assignment['forecast_id']})")

    def get_schedule(self) -> pd.DataFrame:
        """Get the complete schedule as a dataframe"""
        return pd.DataFrame(self.schedule)


class RLGreenScheduler:

    def __init__(self, associations_df, job_list, node_list, optimize_mode='both'):
        self.associations_df = associations_df
        self.node_list = node_list
        self.job_list = job_list
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.optimize_mode = optimize_mode.lower()
        self.max_slot = max(self.time_slots)

        # Initialize output formatter
        self.output_formatter = OutputFormatter(job_list, node_list, self.time_slots)

    def plan(self):
        env = JobSchedulingEnv(self.associations_df, objective='carbon_emissions')

        # Optional: Parallel environments
        # env = make_vec_env(lambda: JobSchedulingEnv(df), n_envs=4)

        # Evaluation callback
        eval_callback = EvalCallback(env, best_model_save_path='./best_model/',
                                     log_path='./logs/', eval_freq=1000,
                                     deterministic=True, render=False)

        # Create model
        model = PPO('MultiInputPolicy', env, verbose=1,
                    tensorboard_log="./tensorboard/")

        # Train
        model.learn(total_timesteps=50000, callback=eval_callback)

        # Save
        model.save("job_scheduler_ppo_carbon")

import torch
import torch.nn as nn
import torch.optim as optim
from torch_geometric.nn import GATv2Conv
import torch.nn.functional as F
from torch_geometric.data import Data
import pandas as pd



class GnnPlanner:
    def __init__(self, associations_df, job_list):
        self.associations_df = associations_df
        self.job_list = job_list

        self.job_sizes = {job['id']: job['bytes'] for job in self.job_list}
        self.job_deadlines = {job['id']: job['deadline'] for job in self.job_list}

        self.route_list = associations_df['route_key'].unique()
        self.time_slots = sorted(associations_df['forecast_id'].unique())
        self.max_slot = max(self.time_slots)
        self.epochs = 100
        self.data, self.job_map, self.route_map = self.prepare_gnn_data()
        self.model = ScheduleGNN()

        # Store throughput (bps) and carbon for output calculations
        self.throughput = dict(zip(
            zip(associations_df['route_key'], associations_df['forecast_id']),
            associations_df['throughput']  # Using throughput in bps
        ))
        self.carbon = dict(zip(
            zip(associations_df['route_key'], associations_df['forecast_id']),
            associations_df['carbon_emissions']
        ))

    def plan(self):
        schedule = self.gnn_optimize()
        print("GNN-Optimized Schedule:")
        print(schedule.sort_values(['job_id', 'forecast_id']))
        print(f"\nTotal Carbon: {schedule['carbon_emissions'].sum():.2f}")
        print(f"Total Allocated Bytes: {schedule['allocated_bytes'].sum():.2f}")

    def prepare_gnn_data(self):
        # Job nodes: [deadline, size_remaining, is_completed]
        job_nodes = torch.tensor([
            [self.job_deadlines[job_id], self.job_sizes[job_id], 0.0]
            for job_id in self.job_map.keys()
        ], dtype=torch.float)

        # Route-time nodes: [forecast_id, throughput] (carbon is NOT here!)
        route_time_nodes = []
        route_time_map = {}  # (route_key, forecast_id) -> node_id
        for (route, time), throughput in self.throughput.items():
            route_time_nodes.append([time, throughput])
            route_time_map[(route, time)] = len(job_nodes) + len(route_time_nodes) - 1

        # Edges: Connect jobs to route-time slots (if time ≤ deadline)
        edge_index = []
        edge_attr = []
        for job_id, job_idx in self.job_map.items():
            deadline = self.job_deadlines[job_id]
            for (route, time), throughput in self.throughput.items():
                if time <= deadline:
                    rt_node = route_time_map.get((route, time))
                    if rt_node is not None:
                        # Get carbon on-demand (from SimGrid or cached)
                        carbon = self.get_carbon(job_id, route, time)  # Implement this!
                        edge_index.append((job_idx, rt_node))
                        edge_attr.append([carbon, throughput])

        x = torch.cat([job_nodes, torch.tensor(route_time_nodes, dtype=torch.float)])
        edge_index = torch.tensor(edge_index, dtype=torch.long).t().contiguous()
        edge_attr = torch.tensor(edge_attr, dtype=torch.float)

        return Data(x=x, edge_index=edge_index, edge_attr=edge_attr)

    def gnn_optimize(self):
        """End-to-end GNN optimization pipeline with fractional allocations"""
        optimizer = optim.Adam(self.model.parameters(), lr=0.001)

        # Inverse mappings
        idx_to_job = {v: k for k, v in self.job_map.items()}
        idx_to_route = {v: k for k, v in self.route_map.items()}

        # Training loop
        for epoch in range(self.epochs):
            self.model.train()
            optimizer.zero_grad()

            # Forward pass
            allocation_scores = self.model(self.data)

            # Custom loss function
            carbon_loss = (allocation_scores.sigmoid() * self.data.edge_attr[:, 0]).mean()
            deadline_loss = self.calculate_deadline_loss(allocation_scores.sigmoid())
            utilization_loss = -allocation_scores.sigmoid().mean()

            # Combine losses with proper scaling
            loss = carbon_loss + deadline_loss + 0.5 * utilization_loss

            # Backpropagation
            loss.backward()
            optimizer.step()

            # Logging
            if epoch % 10 == 0:
                print(f"Epoch {epoch}, Loss: {loss.item():.4f}")

        # Generate final schedule with fractional allocations
        schedule = []
        job_progress = {job_id: 0.0 for job_id in self.job_map.keys()}

        with torch.no_grad():
            allocation_scores = self.model(self.data).sigmoid()

            for job_idx, job_id in idx_to_job.items():
                total_size = self.job_sizes[job_id]
                remaining = total_size - job_progress[job_id]

                if remaining <= 0:
                    continue

                # Get all possible allocations for this job
                job_edges_mask = (self.data.edge_index[0] == job_idx)
                job_edges = job_edges_mask.nonzero().squeeze()

                if job_edges.numel() == 0:  # More robust check for empty edges
                    continue
                if job_edges.dim() == 0:  # Handle case with single edge
                    job_edges = job_edges.unsqueeze(0)

                # Sort edges by score
                sorted_edges = job_edges[torch.argsort(allocation_scores[job_edges], descending=True)]

                for edge_idx in sorted_edges:
                    if remaining <= 0:
                        break

                    src, dst = self.data.edge_index[:, edge_idx]
                    route_key, forecast = idx_to_route[dst.item()]

                    # Calculate maximum possible allocation
                    throughput_bps = self.data.edge_attr[edge_idx, 2]
                    max_possible_bytes = min(remaining, throughput_bps * 3600 / 8)  # Convert bps to bytes/hour

                    # Avoid division by zero
                    denominator = throughput_bps * 3600 / 8
                    if denominator < 1e-6:  # Small epsilon to prevent division by zero
                        continue

                    x_val = min(1.0, max_possible_bytes / denominator)

                    if x_val > 0.01:  # Threshold for meaningful allocations
                        allocated_bytes = x_val * throughput_bps * 3600 / 8
                        schedule.append({
                            'job_id': job_id,
                            'forecast_id': int(forecast),
                            'route_key': route_key,
                            'allocated_fraction': x_val,
                            'allocated_bytes': allocated_bytes,
                            'carbon_emissions': x_val * self.data.edge_attr[edge_idx, 0],
                            'completed': (job_progress[job_id] + allocated_bytes) >= total_size * 0.99
                        })

                        job_progress[job_id] += allocated_bytes
                        remaining = total_size - job_progress[job_id]

        return pd.DataFrame(schedule)

    def calculate_deadline_loss(self, allocations):
        """Penalize allocations that exceed job deadlines"""
        deadline_loss = 0.0
        for job_idx, job_id in self.job_map.items():
            job_edges = (self.data.edge_index[0] == job_idx).nonzero().squeeze()
            if job_edges.dim() == 0:
                continue

            deadline = self.data.x[job_idx, 0]
            late_allocations = allocations[job_edges] * (self.data.edge_attr[job_edges, 1] > deadline).float()
            deadline_loss += late_allocations.sum()

        return deadline_loss / len(self.job_map)

    def get_carbon(self, job_id, route, time):
        pass


class ScheduleGNN(nn.Module):
    def __init__(self, node_feat_dim=3, edge_feat_dim=4, hidden_dim=128):
        super().__init__()
        # Node encoders
        self.job_encoder = nn.Sequential(
            nn.Linear(node_feat_dim, hidden_dim),
            nn.ReLU()
        )
        self.route_encoder = nn.Sequential(
            nn.Linear(node_feat_dim, hidden_dim),
            nn.ReLU()
        )

        # Graph attention
        self.conv1 = GATv2Conv(hidden_dim, hidden_dim, edge_dim=edge_feat_dim)
        self.conv2 = GATv2Conv(hidden_dim, hidden_dim, edge_dim=edge_feat_dim)

        # Scoring network
        self.scorer = nn.Sequential(
            nn.Linear(2 * hidden_dim + edge_feat_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, 1)
        )

    def forward(self, data):
        # Encode nodes
        h_jobs = self.job_encoder(data.x[data.job_mask])
        h_routes = self.route_encoder(data.x[data.route_mask])

        # Combine features
        x = torch.zeros(data.num_nodes, h_jobs.size(1), device=data.x.device)
        x[data.job_mask] = h_jobs
        x[data.route_mask] = h_routes

        # Graph processing
        x = F.relu(self.conv1(x, data.edge_index, data.edge_attr))
        x = F.relu(self.conv2(x, data.edge_index, data.edge_attr))

        # Score edges
        src, dst = data.edge_index
        edge_feats = torch.cat([x[src], x[dst], data.edge_attr], dim=1)
        return self.scorer(edge_feats).squeeze()

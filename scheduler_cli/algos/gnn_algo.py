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
        """Convert dataframe to graph for GNN processing"""
        # Create nodes
        job_nodes = []
        job_map = {}
        for i, job_id in enumerate(self.associations_df['job_id'].unique()):
            job_nodes.append([
                self.job_deadlines[job_id],  # deadline
                self.job_sizes[job_id],  # size in bytes
                0  # current progress
            ])
            job_map[job_id] = i

        route_nodes = []
        route_map = {}
        for i, (route, forecast) in enumerate(
                self.associations_df[['route_key', 'forecast_id']].drop_duplicates().itertuples(index=False)):
            route_data = self.associations_df[
                (self.associations_df['route_key'] == route) &
                (self.associations_df['forecast_id'] == forecast)
                ].iloc[0]
            route_nodes.append([
                route_data['throughput'],  # throughput in bps
                forecast,  # time slot
                route_data['carbon_emissions']  # carbon intensity
            ])
            route_map[(route, forecast)] = i + len(job_map)

        # Create edge
        edges = []
        edge_attrs = []
        for _, row in self.associations_df.iterrows():
            src = job_map[row['job_id']]
            dst = route_map.get((row['route_key'], row['forecast_id']), -1)
            if dst >= 0:
                edges.append((src, dst))
                edge_attrs.append([
                    row['carbon_emissions'],  # Just the intensity (gCO2/kWh), not total emissions
                    row['forecast_id'],
                    row['throughput'],
                    self.job_sizes[row['job_id']]
                ])

        # Convert to tensors
        x = torch.tensor(job_nodes + route_nodes, dtype=torch.float)
        edge_index = torch.tensor(edges, dtype=torch.long).t().contiguous()
        edge_attr = torch.tensor(edge_attrs, dtype=torch.float)

        # Create masks
        job_mask = torch.zeros(x.size(0), dtype=torch.bool)
        job_mask[:len(job_nodes)] = True
        route_mask = ~job_mask

        return Data(x=x, edge_index=edge_index, edge_attr=edge_attr,
                    job_mask=job_mask, route_mask=route_mask), job_map, route_map

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

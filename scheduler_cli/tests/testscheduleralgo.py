import pytest
from click.testing import CliRunner
from scheduler_cli.scheduler_algo import SchedulerAlgo


class TestSchedulerAlgo:

    def test_init_defaults(self):
        test_obj = SchedulerAlgo("../../config/nodes_config.json", "../../config/trace_route_with_coordinates.json", "../../config/jobs.json")
        assert len(test_obj.ip_list) == 14
        assert len(test_obj.job_list) == 3
        assert len(test_obj.node_list) == 4

    def test_load_in_forecasts(self):
        test_obj = SchedulerAlgo("../../config/nodes_config.json", "../../config/trace_route_with_coordinates.json", "../../config/jobs.json")
        test_obj.load_in_forecasts()


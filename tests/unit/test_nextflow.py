import logging
import os
import pytest
from unittest.mock import patch, MagicMock
from biomero.slurm_client import SlurmClient

logging.basicConfig(level=logging.INFO)


@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ["PERSISTENCE_MODULE"] = "eventsourcing_sqlalchemy"
    os.environ["SQLALCHEMY_URL"] = "sqlite:///:memory:"
    yield
    del os.environ["PERSISTENCE_MODULE"]
    del os.environ["SQLALCHEMY_URL"]


@pytest.fixture
@patch('biomero.slurm_client.Connection.create_session')
@patch('biomero.slurm_client.Connection.open')
@patch('biomero.slurm_client.Connection.put')
@patch('biomero.slurm_client.SlurmClient.run')
def slurm_client(_mock_run, _mock_put, _mock_open, _mock_session):
    sc = SlurmClient(
        "localhost", 8022, "slurm",
        nextflow_module="nextflow/23.10",
        nextflow_pipelines_path="my-scratch/nextflow-pipelines",
        nextflow_default_profile="singularity,slurm",
        nextflow_work_path="my-scratch/nextflow-work",
        nextflow_pipelines={"incucyte": "incucyte-analysis"},
        nextflow_repos={
            "incucyte": "https://github.com/Leiden-Cell-Observatory/"
                        "nf-incucyte-analysis"
        },
        nextflow_revisions={"incucyte": "main"},
        nextflow_entries={"incucyte": "main.nf"},
        nextflow_job_params={"incucyte": []},
    )
    return sc


class TestListNextflowPipelines:
    def test_returns_pipeline_names(self, slurm_client):
        result = slurm_client.list_nextflow_pipelines()
        assert result == ["incucyte"]

    def test_empty_when_no_pipelines(self, slurm_client):
        slurm_client.nextflow_pipelines = {}
        assert slurm_client.list_nextflow_pipelines() == []


class TestGetNextflowParameters:
    @patch.object(SlurmClient, 'pull_nextflow_schema')
    def test_parses_schema_correctly(self, mock_schema, slurm_client):
        mock_schema.return_value = {
            "$defs": {
                "input_output": {
                    "title": "Input/Output Options",
                    "type": "object",
                    "properties": {
                        "dataset_id": {
                            "type": "integer",
                            "default": 701,
                            "description": "OMERO Dataset ID",
                        },
                        "container_type": {
                            "type": "string",
                            "default": "plate",
                            "enum": ["plate", "dataset", "project"],
                            "description": "OMERO container type",
                        },
                    },
                    "required": ["dataset_id"],
                }
            }
        }

        params = slurm_client.get_nextflow_parameters("incucyte")

        assert "dataset_id" in params
        assert params["dataset_id"]["type"] == "integer"
        assert params["dataset_id"]["default"] == 701
        assert params["dataset_id"]["optional"] is False
        assert params["dataset_id"]["group"] == "Input/Output Options"

        assert "container_type" in params
        assert params["container_type"]["enum"] == [
            "plate", "dataset", "project"]
        assert params["container_type"]["optional"] is True

    @patch.object(SlurmClient, 'pull_nextflow_schema')
    def test_handles_array_type(self, mock_schema, slurm_client):
        mock_schema.return_value = {
            "$defs": {
                "segmentation": {
                    "title": "Segmentation",
                    "properties": {
                        "channels": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "channel_index": {"type": "integer"},
                                },
                            },
                        },
                    },
                }
            }
        }

        params = slurm_client.get_nextflow_parameters("incucyte")
        assert params["channels"]["type"] == "array"
        assert params["channels"]["items_schema"] is not None


class TestGetNextflowCommand:
    def test_builds_sbatch_command(self, slurm_client):
        slurm_client.slurm_script_path = "my-scratch/slurm-scripts"

        cmd, env, complex_params, full_path = \
            slurm_client.get_nextflow_command(
                "incucyte", dataset_id=701, max_images=10)

        assert "sbatch" in cmd
        assert "nextflow_job_template.sh" in cmd

        assert env["NF_PIPELINE"] == '"incucyte"'
        assert env["NF_PIPELINE_PATH"] == (
            '"my-scratch/nextflow-pipelines/incucyte-analysis"')
        assert env["NF_ENTRY"] == '"main.nf"'
        assert env["NF_PROFILE"] == '"singularity,slurm"'

        assert "--dataset_id 701" in env["NF_PARAMS"]
        assert "--max_images 10" in env["NF_PARAMS"]
        assert complex_params == {}
        assert full_path == "my-scratch/nextflow-pipelines/incucyte-analysis"

    def test_separates_complex_params(self, slurm_client):
        slurm_client.slurm_script_path = "my-scratch/slurm-scripts"

        channels = [{"channel_index": 0, "channel_name": "GFP"}]
        cmd, env, complex_params, _ = slurm_client.get_nextflow_command(
            "incucyte", dataset_id=701, stardist_channels=channels)

        assert "stardist_channels" in complex_params
        assert complex_params["stardist_channels"] == channels
        assert "NF_PARAMS_FILE" in env
        # Simple params should still be there
        assert "--dataset_id 701" in env["NF_PARAMS"]

    def test_bool_params_lowercase(self, slurm_client):
        slurm_client.slurm_script_path = "my-scratch/slurm-scripts"

        cmd, env, _, _path = slurm_client.get_nextflow_command(
            "incucyte", upload_to_omero=True)

        assert "--upload_to_omero true" in env["NF_PARAMS"]


class TestPullNextflowSchema:
    def test_raises_for_unknown_pipeline(self, slurm_client):
        with pytest.raises(ValueError, match="not configured"):
            slurm_client.pull_nextflow_schema("nonexistent")

    def test_raises_for_non_github_url(self, slurm_client):
        slurm_client.nextflow_repos["test"] = "https://gitlab.com/foo/bar"
        with pytest.raises(ValueError, match="Invalid GitHub URL"):
            slurm_client.pull_nextflow_schema("test")

    @patch.object(SlurmClient, 'get_or_create_github_session')
    def test_fetches_schema_from_github(self, mock_session, slurm_client):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"$defs": {}}
        mock_session.return_value.get.return_value = mock_response

        result = slurm_client.pull_nextflow_schema("incucyte")
        assert result == {"$defs": {}}

        called_url = mock_session.return_value.get.call_args[0][0]
        assert "raw.githubusercontent.com" in called_url
        assert "nextflow_schema.json" in called_url


class TestRunNextflowPipeline:
    @patch.object(SlurmClient, 'extract_job_id', return_value=12345)
    @patch.object(SlurmClient, 'run_commands')
    @patch.object(SlurmClient, 'get_nextflow_command')
    def test_submits_pipeline(self, mock_cmd, mock_run, mock_extract,
                              slurm_client):
        mock_cmd.return_value = (
            "sbatch ...", {"NF_PIPELINE": "x"}, {},
            "my-scratch/nextflow-pipelines/incucyte-analysis")
        mock_result = MagicMock()
        mock_result.ok = True
        mock_run.return_value = mock_result

        # Mock the workflow tracker to avoid serialization issues
        slurm_client.workflowTracker = MagicMock()
        slurm_client.workflowTracker.initiate_workflow.return_value = \
            "wf-id-123"
        slurm_client.workflowTracker.add_task_to_workflow.return_value = \
            "task-id-456"

        res, job_id, wf_id, task_id = slurm_client.run_nextflow_pipeline(
            "incucyte", dataset_id=701)

        assert job_id == 12345
        assert res.ok is True
        mock_cmd.assert_called_once_with("incucyte", dataset_id=701)
        slurm_client.workflowTracker.start_task.assert_called_once()
        slurm_client.workflowTracker.add_job_id.assert_called_once_with(
            "task-id-456", 12345)


class TestSetupNextflowPipelines:
    @patch.object(SlurmClient, 'run_commands')
    def test_clones_pipelines(self, mock_run, slurm_client):
        mock_run.return_value = MagicMock(ok=True)

        slurm_client.setup_nextflow_pipelines()

        # Should create base dirs + run clone/pull for each pipeline
        assert mock_run.call_count >= 2

    @patch.object(SlurmClient, 'run_commands')
    def test_skips_when_no_path(self, mock_run, slurm_client):
        slurm_client.nextflow_pipelines_path = None

        slurm_client.setup_nextflow_pipelines()

        mock_run.assert_not_called()

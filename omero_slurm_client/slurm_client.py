from typing import Dict, List, Optional, Tuple
from fabric import Connection, Result
from fabric.transfer import Result as TransferResult
from paramiko import SSHException
import configparser
import re


class SlurmClient(Connection):
    """A client for connecting to and interacting with a Slurm cluster over 
    SSH.

    This class extends the Connection class, adding methods and 
    attributes specific to working with Slurm.

    SlurmClient accepts the same arguments as Connection. So below only 
    mentions the added ones:

    Attributes:
        slurm_data_path (str): The path to the directory containing the data files for Slurm jobs.
        slurm_images_path (str): The path to the directory containing the Singularity images for Slurm jobs.
        slurm_model_paths (dict): A dictionary containing the paths to the Singularity images for specific Slurm job models.
        slurm_model_repos (dict): A dictionary containing the git repositories of Singularity images for specific Slurm job models.
        slurm_model_images (dict): A dictionary containing the dockerhub of the Singularity images for specific Slurm job models.
        slurm_script_path (str): The path to the directory containing the Slurm job submission scripts. This is expected to be a Git repository.

    Example:
        # Create a SlurmClient object as contextmanager

        with SlurmClient.from_config() as client:

            # Run a command on the remote host

            result = client.run('sbatch myjob.sh')

            # Check whether the command succeeded

            if result.ok:
                print('Job submitted successfully!')

            # Print the output of the command

            print(result.stdout)

    """
    _DEFAULT_CONFIG_PATH_1 = "/etc/slurm-config.ini"
    _DEFAULT_CONFIG_PATH_2 = "~/slurm-config.ini"
    _DEFAULT_HOST = "slurm"
    _DEFAULT_INLINE_SSH_ENV = True
    _DEFAULT_SLURM_DATA_PATH = "my-scratch/data"
    _DEFAULT_SLURM_IMAGES_PATH = "my-scratch/singularity_images/workflows"
    _DEFAULT_SLURM_GIT_SCRIPT_PATH = "slurm-scripts"
    _OUT_SEP = "--split--"
    _VERSION_CMD = "ls -h {slurm_images_path}/{image_path} | grep -oP '(?<=-)v.+(?=.simg)'"
    _DATA_CMD = "ls -h {slurm_data_path} | grep -oP '.+(?=.zip)'"
    _ALL_JOBS_CMD = "sacct --starttime {start_time} --endtime {end_time} --state {states} -o {columns} -n -X "
    _ZIP_CMD = "7z a -y {filename} -tzip {data_location}/data/out"
    _ACTIVE_JOBS_CMD = "squeue -u $USER --nohead --format %F"
    _JOB_STATUS_CMD = "sacct -n -o JobId,State,End -X -j {slurm_job_id}"
    # TODO move all commands to a similar format.
    # Then maybe allow overwrite from slurm-config.ini
    _LOGFILE = "omero-{slurm_job_id}.log"
    _TAIL_LOG_CMD = "tail -n {n} {log_file} | strings"

    def __init__(self,
                 host=_DEFAULT_HOST,
                 user=None,
                 port=None,
                 config=None,
                 gateway=None,
                 forward_agent=None,
                 connect_timeout=None,
                 connect_kwargs=None,
                 inline_ssh_env=_DEFAULT_INLINE_SSH_ENV,
                 slurm_data_path: str = _DEFAULT_SLURM_DATA_PATH,
                 slurm_images_path: str = _DEFAULT_SLURM_IMAGES_PATH,
                 slurm_model_paths: dict = None,
                 slurm_model_repos: dict = None,
                 slurm_model_images: dict = None,
                 slurm_model_jobs: dict = None,
                 slurm_script_path: str = _DEFAULT_SLURM_GIT_SCRIPT_PATH
                 ):
        super(SlurmClient, self).__init__(host,
                                          user,
                                          port,
                                          config,
                                          gateway,
                                          forward_agent,
                                          connect_timeout,
                                          connect_kwargs,
                                          inline_ssh_env)
        self.slurm_data_path = slurm_data_path
        self.slurm_images_path = slurm_images_path
        self.slurm_model_paths = slurm_model_paths
        self.slurm_script_path = slurm_script_path
        self.slurm_model_repos = slurm_model_repos
        self.slurm_model_images = slurm_model_images
        self.slurm_model_jobs = slurm_model_jobs
        # TODO: setup the script path by downloading from GIT? setup all the directories?

    @classmethod
    def from_config(cls, configfile: str = '') -> 'SlurmClient':
        """Creates a new SlurmClient object using the parameters read from a configuration file (.ini).

        Defaults paths to look for config files are:
            - /etc/slurm-config.ini
            - ~/slurm-config.ini

        Note that this is only for the SLURM specific values that we added.
        Most configuration values are set via configuration mechanisms from Fabric library,
        like SSH settings being loaded from SSH config, /etc/fabric.yml or environment variables.
        See Fabric's documentation for more info on configuration if needed.

        Args:
            configfile (str): The path to your configuration file. Optional.

        Returns:
            SlurmClient: A new SlurmClient object.
        """
        # Load the configuration file
        configs = configparser.ConfigParser(allow_no_value=True)
        # Loads from default locations and given location, missing files are ok
        configs.read([cls._DEFAULT_CONFIG_PATH_1,
                     cls._DEFAULT_CONFIG_PATH_2, configfile])
        # Read the required parameters from the configuration file, fallback to defaults
        host = configs.get("SSH", "host", fallback=cls._DEFAULT_HOST)
        inline_ssh_env = configs.getboolean(
            "SSH", "inline_ssh_env", fallback=cls._DEFAULT_INLINE_SSH_ENV)
        slurm_data_path = configs.get(
            "SLURM", "slurm_data_path", fallback=cls._DEFAULT_SLURM_DATA_PATH)
        slurm_images_path = configs.get(
            "SLURM", "slurm_images_path", fallback=cls._DEFAULT_SLURM_IMAGES_PATH)
        # Split the MODELS into paths, repos and images
        models_dict = dict(configs.items("MODELS"))
        slurm_model_paths = {}
        slurm_model_repos = {}
        slurm_model_images = {}
        slurm_model_jobs = {}
        for k, v in models_dict.items():
            suffix_repo = '_repo'
            suffix_image = '_image'
            suffix_job = '_job'
            if k.endswith(suffix_repo):
                slurm_model_repos[k[:-len(suffix_repo)]] = v
            elif k.endswith(suffix_image):
                slurm_model_images[k[:-len(suffix_image)]] = v
            elif k.endswith(suffix_job):
                slurm_model_jobs[k[:-len(suffix_job)]] = v
            else:
                slurm_model_paths[k] = v
        
        slurm_script_path = configs.get(
            "SLURM", "slurm_script_path", fallback=cls._DEFAULT_SLURM_GIT_SCRIPT_PATH)
        # Create the SlurmClient object with the parameters read from the config file
        return cls(host=host,
                   inline_ssh_env=inline_ssh_env,
                   slurm_data_path=slurm_data_path,
                   slurm_images_path=slurm_images_path,
                   slurm_model_paths=slurm_model_paths,
                   slurm_model_repos=slurm_model_repos,
                   slurm_model_images=slurm_model_images,
                   slurm_model_jobs=slurm_model_jobs,
                   slurm_script_path=slurm_script_path)

    def validate(self):
        """Validate the connection to the Slurm cluster by running a simple command.

        Returns:
            bool: True if the command is executed successfully, False otherwise.
        """
        return self.run('echo " "').ok

    def get_recent_log_command(self, log_file: str, n: int = 10) -> str:
        return self._TAIL_LOG_CMD.format(n=n, log_file=log_file)

    def get_active_job_progress(self,
                                slurm_job_id: str,
                                pattern: str = "\d+%",
                                env: Optional[Dict[str, str]] = None) -> str:
        cmdlist = []
        cmd = self.get_recent_log_command(
            log_file=self._LOGFILE.format(slurm_job_id=slurm_job_id))
        cmdlist.append(cmd)
        # with settings():
        if env is None:
            env = {}
        # env["LC_ALL"] = "en_US.UTF-8"
        try:
            result = self.run_commands(cmdlist, env=env)
            #    out_stream=TextIOWrapper(sys.stdout.buffer, encoding='utf8'))
        except Exception as e:
            print(f"Issue with run command: {e}")
        # match some pattern
        try:
            # result.stdout = result.stdout.encode('ascii', 'ignore').decode('ascii')
            latest_progress = re.findall(
                pattern, result.stdout)[-1]
        except Exception as e:
            print(f"Issue with print commands: {e}")

        return f"Progress: {latest_progress}\n"

    def run_commands(self, cmdlist: List[str], env: Optional[Dict[str, str]] = None, sep: str = ' && ', out_stream=None, **kwargs) -> Result:
        """
        Runs a list of shell commands consecutively, ensuring success of each before calling the next.

        The environment variables can be set using the `env` argument. These commands retain the same session (environment variables
        etc.), unlike running them separately.

        Args:
            cmdlist (List[str]): A list of shell commands to run on SLURM.
            env (Optional[Dict[str, str]]): A dictionary of environment variables to be set for the commands (default: None).
            sep (str): The separator used to concatenate the commands (default: ' && ').

        Returns:
            Result: The result of the last command in the list.
        """
        if env is None:
            env = {}
        # if out_stream is None:
        #     out_stream = TextIOWrapper(sys.stdout.buffer, encoding='utf8')
        cmd = sep.join(cmdlist)
        print(
            f"Running commands, with env {env} and sep {sep} and {kwargs}: {cmd}")
        result = self.run(cmd, env=env, **kwargs)  # out_stream=out_stream,

        try:
            # Watch out for UnicodeEncodeError when you str() this, so just ascii it now.
            print(f"{result.stdout}")
        except UnicodeEncodeError as e:
            print(f"Unicode error: {e}")
            # TODO: ONLY THIS RECODE NEEDED?? don't know
            result.stdout = result.stdout.encode(
                'utf-8', 'ignore').decode('utf-8')
        return result

    def run_commands_split_out(self, cmdlist: List[str], env: Optional[Dict[str, str]] = None) -> List[str]:
        """Runs a list of shell commands consecutively and splits the output of each command.

        Each command in the list is executed with a separator in between that is unique and can be used to split
        the output of each command later. The separator used is specified by the `_OUT_SEP` attribute of the
        SlurmClient instance.

        Args:
            cmdlist (List[str]): A list of shell commands to run.
            env (Optional[Dict[str, str]]): A dictionary of environment variables to set when running the commands.

        Returns:
            List[str]: A list of strings, where each string corresponds to the output of a single command
                    in `cmdlist` split by the separator `_OUT_SEP`.
        Raises:
            SSHException: If any of the commands fail to execute successfully.
        """
        result = self.run_commands(cmdlist=cmdlist,
                                   env=env,
                                   sep=f" ; echo {self._OUT_SEP} ; ")
        if result.ok:
            response = result.stdout
            split_responses = response.split(self._OUT_SEP)
            return split_responses
        else:
            error = f"Result is not ok: {result}"
            print(error)
            raise SSHException(error)

    def list_active_jobs(self, env: Optional[Dict[str, str]] = None) -> List[str]:
        """Get list of active jobs from SLURM.

        Args:
            env (Optional[Dict[str, str]]): Optional environment variables to set when running the command.
                Defaults to None.

        Returns:
            List: List of Job Ids
        """
        # cmd = self._ACTIVE_JOBS_CMD
        cmd = self.get_jobs_info_command(start_time="now", states="r")
        print("Retrieving list of active jobs from Slurm")
        result = self.run_commands([cmd], env=env)
        job_list = result.stdout.strip().split('\n')
        job_list.reverse()
        return job_list

    def list_completed_jobs(self, env: Optional[Dict[str, str]] = None) -> List[str]:
        """Get list of jobs from SLURM.

        Args:
            env (Optional[Dict[str, str]]): Optional environment variables to set when running the command.
                Defaults to None.

        Returns:
            List: List of Job Ids
        """

        cmd = self.get_jobs_info_command(states="cd")
        print("Retrieving list of jobs from Slurm")
        result = self.run_commands([cmd], env=env)
        job_list = result.stdout.strip().split('\n')
        job_list.reverse()
        return job_list

    def list_all_jobs(self, env: Optional[Dict[str, str]] = None) -> List[str]:
        """Get list of jobs from SLURM.

        Args:
            env (Optional[Dict[str, str]]): Optional environment variables to set when running the command.
                Defaults to None.

        Returns:
            List: List of Job Ids
        """

        cmd = self.get_jobs_info_command()
        print("Retrieving list of jobs from Slurm")
        result = self.run_commands([cmd], env=env)
        job_list = result.stdout.strip().split('\n')
        job_list.reverse()
        return job_list

    def get_jobs_info_command(self, start_time: str = "2023-01-01",
                              end_time: str = "now",
                              columns: str = "JobId",
                              states: str = "r,cd,f,to,rs,dl,nf") -> str:
        """Return the Slurm command to retrieve information about old jobs.

        The command will be formatted with the specified start time, which is
        expected to be in the ISO format "YYYY-MM-DD".
        The command will use the "sacct" tool to query the
        Slurm accounting database for jobs that started on or after the
        specified start time, and will output only the job IDs (-o JobId)
        without header or trailer lines (-n -X).

        Args:
            start_time (str): The start time from which to retrieve job information.
                Defaults to "2023-01-01".

        Returns:
            str: A string representing the Slurm command to retrieve information
                about old jobs.
        """
        return self._ALL_JOBS_CMD.format(start_time=start_time,
                                         end_time=end_time,
                                         states=states,
                                         columns=columns)

    def transfer_data(self, local_path: str) -> Result:
        """Transfers a file or directory from the local machine to the remote Slurm cluster.

        Args:
            local_path (str): The local path to the file or directory to transfer.

        Returns:
            Result: The result of the file transfer operation.
        """
        print(
            f"Transfering file {local_path} to {self.slurm_data_path}")
        return self.put(local=local_path, remote=self.slurm_data_path)

    def unpack_data(self, zipfile: str, env: Optional[Dict[str, str]] = None) -> Result:
        """Unpacks a zipped file on the remote Slurm cluster.

        Args:
            zipfile (str): The name of the zipped file to be unpacked.
            env (Optional[Dict[str, str]]): Optional environment variables to set when running the command.
                Defaults to None.

        Returns:
            Result: The result of the command.

        """
        cmd = self.get_unzip_command(zipfile)
        print(f"Unpacking {zipfile} on Slurm")
        return self.run_commands([cmd], env=env)

    def update_slurm_scripts(self, env: Optional[Dict[str, str]] = None) -> Result:
        """Updates the local copy of the Slurm job submission scripts.

        This function pulls the latest version of the scripts from the Git repository,
        and copies them to the slurm_script_path directory.

        Args:
            env (Optional[Dict[str, str]]): Optional environment variables to set when running the command.
                Defaults to None.

        Returns:
            Result: The result of the command.
        """
        cmd = self.get_update_slurm_scripts_command()
        print("Updating Slurm job scripts on Slurm")
        return self.run_commands([cmd], env=env)

    def run_cellpose(self, cellpose_version: str, input_data: str,
                     cp_model: str, nuc_channel: int,
                     prob_threshold: float,
                     cell_diameter: int, email: Optional[str] = None,
                     time: Optional[str] = None) -> Result:
        """
        Runs CellPose on Slurm on the specified input data using the given parameters.

        Args:
            cellpose_version (str): The version of CellPose to use.
            input_data (str): The name of the input data folder containing the input image files.
            cp_model (str): The name of the CellPose model to use for segmentation.
            nuc_channel (int): The index of the nuclear channel in the image data.
            prob_threshold (float): The threshold probability value for object segmentation.
            cell_diameter (int): The approximate diameter of the cells in pixels.
            email (Optional[str]): The email address to use for Slurm job notifications.
            time (Optional[str]): The time limit for the Slurm job in the format HH:MM:SS.

        Returns:
            Result: An object containing the output from starting the CellPose job.
            int: The jobid from Slurm, or -1 if it could not be extracted

        """
        sbatch_cmd, sbatch_env = self.get_cellpose_command(
            cellpose_version, input_data, cp_model, nuc_channel,
            prob_threshold, cell_diameter, email, time)
        print("Running CellPose job on Slurm")
        res = self.run_commands([sbatch_cmd], sbatch_env)
        return res, self.extract_job_id(res)

    def extract_job_id(result: Result) -> int:
        slurm_job_id = next((int(s.strip()) for s in result.stdout.split(
                            "Submitted batch job") if s.strip().isdigit()), -1)
        return slurm_job_id

    def get_update_slurm_scripts_command(self) -> str:
        """Generates the command to update the Git repository containing the Slurm scripts, if necessary.

        Returns:
            str: A string containing the Git command to update the Slurm scripts.
        """
        update_cmd = f"git -C {self.slurm_script_path} pull"
        return update_cmd

    def check_job_status(self, slurm_job_id: str, env: Optional[Dict[str, str]] = None) -> Result:
        """
        Checks the status of a Slurm job with the given job ID.

        Args:
            slurm_job_id (str): The job ID of the Slurm job to check.
            env (Optional[Dict[str, str]]): A dictionary of environment variables to set before executing the command. Defaults to None.

        Returns:
            Result: The result of the command execution.
        """
        cmd = self.get_job_status_command(slurm_job_id)
        print(f"Getting status of {slurm_job_id} on Slurm")
        return self.run_commands([cmd], env=env)

    def get_job_status_command(self, slurm_job_id: str) -> str:
        """
        Returns the Slurm command to get the status of a job with the given job ID.

        Args:
            slurm_job_id (str): The job ID of the job to check.

        Returns:
            str: The Slurm command to get the status of the job.
        """
        return self._JOB_STATUS_CMD.format(slurm_job_id=slurm_job_id)
    
    def get_workflow_command(self, workflow, *args, **kwargs) -> Tuple[str, dict]:
        model_path = self.slurm_model_paths[workflow]
        git_repo = self.slurm_model_repos[workflow]
        image_repo = self.slurm_model_images[workflow]
        job_script = self.slurm_model_jobs[workflow]
        sbatch_env = {
            "DATA_PATH": f"{self.slurm_data_path}/{input_data}",
            "IMAGE_PATH": f"{self.slurm_images_path}/{model_path}",
            "IMAGE_VERSION": f"{image_version}",
        }
        cellpose_env = {
            "DIAMETER": f"{cell_diameter}",
            "PROB_THRESHOLD": f"{prob_threshold}",
            "NUC_CHANNEL": f"{nuc_channel}",
            "CP_MODEL": f"{cp_model}",
            "USE_GPU": "true",
        }
        env = {**sbatch_env, **cellpose_env}

        email_param = "" if email is None else f" --mail-user={email}"
        time_param = "" if time is None else f" --time={time}"
        job_params = [time_param, email_param]
        job_param = "".join(job_params)
        sbatch_cmd = f"sbatch{job_param} --output=omero-%4j.log {self.slurm_script_path}/{job_script}"

        return sbatch_cmd, env
        

    def get_cellpose_command(self, image_version, input_data, cp_model, nuc_channel, prob_threshold, cell_diameter, email=None, time=None, model="cellpose", job_script="cellpose.sh") -> Tuple[str, dict]:
        """
        Returns the command and environment dictionary to run a CellPose job on the Slurm workload manager.

        Args:
            image_version (str): The version of the Singularity image to use.
            input_data (str): The name of the input data folder on the shared file system.
            cp_model (str): The name of the CellPose model to use.
            nuc_channel (int): The index of the nuclear channel.
            prob_threshold (float): The probability threshold for nuclei detection.
            cell_diameter (float): The expected cell diameter in pixels.
            email (Optional[str]): The email address to send notifications to (default is None).
            time (Optional[str]): The maximum time for the job to run (default is None).
            model (str): The name of the folder of the Docker image to use (default is "cellpose").
            job_script (str): The name of the Slurm job script to use (default is "cellpose.sh").

        Returns:
            Tuple[str, dict]: A tuple containing the Slurm sbatch command and the environment dictionary.

        """
        sbatch_env = {
            "DATA_PATH": f"{self.slurm_data_path}/{input_data}",
            "IMAGE_PATH": f"{self.slurm_images_path}/{model}",
            "IMAGE_VERSION": f"{image_version}",
        }
        cellpose_env = {
            "DIAMETER": f"{cell_diameter}",
            "PROB_THRESHOLD": f"{prob_threshold}",
            "NUC_CHANNEL": f"{nuc_channel}",
            "CP_MODEL": f"{cp_model}",
            "USE_GPU": "true",
        }
        env = {**sbatch_env, **cellpose_env}

        email_param = "" if email is None else f" --mail-user={email}"
        time_param = "" if time is None else f" --time={time}"
        job_params = [time_param, email_param]
        job_param = "".join(job_params)
        sbatch_cmd = f"sbatch{job_param} --output=omero-%4j.log {self.slurm_script_path}/jobs/{job_script}"

        return sbatch_cmd, env

    def copy_zip_locally(self, local_tmp_storage: str, filename: str) -> TransferResult:
        """ Copy zip from SLURM to local server

        Note about (Transfer)Result:

        Unlike similar classes such as invoke.runners.Result or fabric.runners.Result 
        (which have a concept of “warn and return anyways on failure”) this class has no useful truthiness behavior. 
        If a file transfer fails, some exception will be raised, either an OSError or an error from within Paramiko.

        Args:
            local_tmp_storage (String): Path to store zip
            filename (String): Zip filename on Slurm
        """
        print(f"Copying zip {filename} from Slurm to {local_tmp_storage}")
        return self.get(
            remote=f"{filename}.zip",
            local=local_tmp_storage)

    def zip_data_on_slurm_server(self, data_location: str, filename: str, env: Optional[Dict[str, str]] = None) -> Result:
        """Zip the output folder of a job on SLURM

        Args:
            data_location (String): Folder on SLURM with the "data/out" subfolder
            filename (String): Name to give to the zipfile
        """
        # zip
        zip_cmd = self.get_zip_command(data_location, filename)
        print(f"Zipping {data_location} as {filename} on Slurm")
        return self.run_commands([zip_cmd], env=env)

    def get_zip_command(self, data_location: str, filename: str) -> str:
        return self._ZIP_CMD.format(filename=filename, data_location=data_location)

    def get_logfile_from_slurm(self, slurm_job_id: str, local_tmp_storage: str = "/tmp/", logfile: str = None) -> Tuple[str, str, TransferResult]:
        """Copy the logfile of given SLURM job to local server

        Note about (Transfer)Result:

        Unlike similar classes such as invoke.runners.Result or fabric.runners.Result 
        (which have a concept of “warn and return anyways on failure”) this class has no useful truthiness behavior. 
        If a file transfer fails, some exception will be raised, either an OSError or an error from within Paramiko.

        Args:
            slurm_job_id (String): ID of the SLURM job

        Returns:
            Tuple: directory, full path of the logfile, and TransferResult
        """
        if logfile is None:
            logfile = self._LOGFILE
        logfile = logfile.format(slurm_job_id=slurm_job_id)
        print(f"Copying logfile {logfile} from Slurm to {local_tmp_storage}")
        result = self.get(
            remote=logfile,
            local=local_tmp_storage)
        export_file = local_tmp_storage+logfile
        return local_tmp_storage, export_file, result

    def get_unzip_command(self, zipfile: str, filter_filetypes: str = "*.tiff *.tif") -> str:
        """
        Generate a command string for unzipping a data archive and creating 
        required directories for Slurm jobs.

        Args:
            zipfile (str): The name of the zip archive file to extract. Without extension.
            filter_filetypes (str, optional): A space-separated string containing the file extensions to extract
            from the zip file. The default value is "*.tiff *.tif".
            Setting this argument to `None` or '*' will omit the file filter and extract all files.

        Returns:
            str: The command to extract the specified filetypes from the zip file.

        """
        if filter_filetypes is None:
            filter_filetypes = '*'  # omit filter
        unzip_cmd = f"mkdir {self.slurm_data_path}/{zipfile} \
                    {self.slurm_data_path}/{zipfile}/data \
                    {self.slurm_data_path}/{zipfile}/data/in \
                    {self.slurm_data_path}/{zipfile}/data/out \
                    {self.slurm_data_path}/{zipfile}/data/gt; \
                    7z e -y -o{self.slurm_data_path}/{zipfile}/data/in \
                    {self.slurm_data_path}/{zipfile}.zip {filter_filetypes}"

        return unzip_cmd

    def get_image_versions_and_data_files(self, model: str) -> Tuple[List[str], List[str]]:
        """
        Gets the available image versions and (input) data files for a given model.

        Args:
            model (str): The name of the model to query for.

        Returns:
            Tuple[List[str], List[str]]: A tuple of 2 lists, the first containing the available image versions
            and the second containing the available data files.
        Raises:
            ValueError: If the provided model is not found in the SlurmClient's known model paths.
        """
        try:
            image_path = self.slurm_model_paths.get(model)
        except KeyError:
            raise ValueError(
                f"No path known for provided model {model}, in {self.slurm_model_paths}")
        cmdlist = [self._VERSION_CMD.format(slurm_images_path=self.slurm_images_path,
                                            image_path=image_path),
                   self._DATA_CMD.format(slurm_data_path=self.slurm_data_path)]
        # split responses per command
        response_list = self.run_commands_split_out(cmdlist)
        # split lines further into sublists
        response_list = [response.strip().split('\n')
                         for response in response_list]
        return response_list[0], response_list[1]

    def get_all_image_versions_and_data_files(self) -> dict[str, str]:
        resultdict = {}
        cmdlist = []
        for path in self.slurm_model_paths.values():
            pathcmd = self._VERSION_CMD.format(slurm_images_path=self.slurm_images_path,
                                               image_path=path)
            cmdlist.append(pathcmd)
        # Add data path too
        cmdlist.append(self._DATA_CMD.format(
            slurm_data_path=self.slurm_data_path))
        # split responses per command
        response_list = self.run_commands_split_out(cmdlist)
        # split lines further into sublists
        response_list = [response.strip().split('\n')
                         for response in response_list]
        for i, k in enumerate(self.slurm_model_paths):
            # return highest version first
            resultdict[k] = sorted(response_list[i], reverse=True)
        # add data paths too
        resultdict['data'] = response_list[-1]
        return resultdict

#!/bin/bash

##############################
#   Nextflow Job Wrapper     #
##############################
# This script wraps a Nextflow pipeline execution inside a Slurm job.
# Nextflow itself will submit sub-jobs to Slurm for each process.
# BIOMERO tracks this wrapper job ID for monitoring.

# Give your job a name
#SBATCH --job-name=omero-nf

# Lightweight resources for the Nextflow master process
#SBATCH --cpus-per-task=2

# Long timeout - Nextflow pipelines can run for hours/days
#              d-hh:mm:ss
#SBATCH --time=2-00:00:00

# Modest memory for the orchestrator
#SBATCH --mem=4GB

# Log file compatible with BIOMERO's log parsing
#SBATCH --output=omero-%j.log
#SBATCH --open-mode=append

# Mail notification
#SBATCH --mail-type=END,FAIL

##############################
#       Job script           #
##############################

echo "Running Nextflow pipeline $NF_PIPELINE | $NF_ENTRY | $NF_WORK_PATH"

# Load required modules
echo "Loading Nextflow and Singularity modules..."
module load ${NF_MODULE:-nextflow} > /dev/null 2>&1 || true
module load singularity > /dev/null 2>&1 || true

# Setup Nextflow work directory with job ID for isolation
export NXF_WORK="${NF_WORK_PATH}/${SLURM_JOB_ID}"
mkdir -p "$NXF_WORK"

# Change to pipeline directory
cd "$NF_PIPELINE_PATH"
mkdir -p reports

# Build the Nextflow command
NF_CMD="nextflow run $NF_ENTRY"
NF_CMD="$NF_CMD -profile $NF_PROFILE"
NF_CMD="$NF_CMD -work-dir $NXF_WORK"
NF_CMD="$NF_CMD -resume"
NF_CMD="$NF_CMD -with-report reports/report_${SLURM_JOB_ID}.html"
NF_CMD="$NF_CMD -with-trace reports/trace_${SLURM_JOB_ID}.txt"

# Add params file if provided (for complex parameters)
if [ -n "$NF_PARAMS_FILE" ] && [ -f "$NF_PARAMS_FILE" ]; then
    NF_CMD="$NF_CMD -params-file $NF_PARAMS_FILE"
fi

# Add simple CLI parameters
if [ -n "$NF_PARAMS" ]; then
    NF_CMD="$NF_CMD $NF_PARAMS"
fi

echo "Executing: $NF_CMD"

# Run Nextflow - it will submit its own Slurm sub-jobs
eval $NF_CMD && echo "Job completed successfully." \
    || { echo "Pipeline failed with exit code $?"; exit 1; }

#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p long                                                        # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 1                                                           # Number of cores (processors)
#SBATCH -t 168:00:00                                                   # Walltime/duration of job
#SBATCH --mem=4G                                                       # Memory per node in GB needed for a job. Also see --mem-per-cpu
#SBATCH --output=./outfiles/write_openalex_filepaths.out               # Path for output must already exist
#SBATCH --error=./outfiles/write_openalex_filepaths.err                # Path for error must already exist
#SBATCH --job-name="OpenAlex filepaths"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from create_subcorpora import write_openalex_filepaths; write_openalex_filepaths()"
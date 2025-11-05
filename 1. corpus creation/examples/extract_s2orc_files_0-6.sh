#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p normal                                                      # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 1                                                           # Number of cores (processors)
#SBATCH -t 24:00:00                                                    # Walltime/duration of job
#SBATCH --mem=4G                                                       # Memory per node in GB needed for a job. Also see --mem-per-cpu
#SBATCH --output=./outfiles/extract_s2orc_files_0-6.out                # Path for output must already exist
#SBATCH --error=./outfiles/extract_s2orc_files_0-6.err                 # Path for error must already exist
#SBATCH --job-name="S2ORC 0-6"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from create_subcorpora import extract_from_s2orc; extract_from_s2orc(start=0, end=6)"
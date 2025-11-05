#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p long                                                        # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 1                                                           # Number of cores (processors)
#SBATCH -t 168:00:00                                                   # Walltime/duration of job
#SBATCH --mem=4G                                                       # Memory per node in GB needed for a job. Also see --mem-per-cpu
#SBATCH --output=./outfiles/get_openalex_info_0-1846.out               # Path for output must already exist
#SBATCH --error=./outfiles/get_openalex_info_0-1846.err                # Path for error must already exist
#SBATCH --job-name="OpenAlex for subdirs 0-1846"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from create_subcorpora import get_openalex_info; get_openalex_info(start=0, end=1846, verbose=True)"
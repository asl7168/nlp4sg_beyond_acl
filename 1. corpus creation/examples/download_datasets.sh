#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p normal                                                      # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 1                                                           # Number of cores (processors)
#SBATCH -t 24:00:00                                                    # Walltime/duration of job
#SBATCH --mem=4G                                                       # Memory per CPU
#SBATCH --output=./outfiles/download_datasets.out                      # Path for output must already exist
#SBATCH --error=./outfiles/download_datasets.err                       # Path for error must already exist
#SBATCH --job-name="Download Semantic Scholar datasets"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from create_subcorpora import download_s2orc; download_s2orc()"
python -c "from create_subcorpora import download_s2_papers; download_s2_papers()"
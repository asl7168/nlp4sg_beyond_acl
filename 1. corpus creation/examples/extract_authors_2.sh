#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p normal                                                      # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 10                                                          # Number of cores (processors)
#SBATCH -t 16:00:00                                                    # Walltime/duration of job
#SBATCH --mem-per-cpu=8G                                               # Memory per CPU
#SBATCH --output=./outfiles/extract_authors_2.out                      # Path for output must already exist
#SBATCH --error=./outfiles/extract_authors_2.err                       # Path for error must already exist
#SBATCH --job-name="Getting author info"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from create_subcorpora import extract_authors_2; extract_authors_2()"
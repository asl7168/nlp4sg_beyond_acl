#!/bin/bash

#SBATCH -A p31502                                                      # Allocation
#SBATCH -p long                                                        # Queue
#SBATCH -N 1                                                           # Number of nodes
#SBATCH -n 1                                                           # Number of cores (processors)
#SBATCH -t 168:00:00                                                   # Walltime/duration of job
#SBATCH --mem=8G                                                       # Memory per node in GB needed for a job. Also see --mem-per-cpu
#SBATCH --output=./outfiles/make_csv_2273-2466.out                     # Path for output must already exist
#SBATCH --error=./outfiles/make_csv_2273-2466.err                      # Path for error must already exist
#SBATCH --job-name="Making CSV 2273-2466"

conda activate nlp4sg
cd /projects/p31502/projects/nlp4sg/1.\ corpus\ creation
python -c "from csv_builder import csv_builder; csv_builder(start=2273, end=2466)"
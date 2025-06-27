#!/bin/bash
#PBS -l walltime=01:30:00
#PBS -l select=1:ncpus=1:mem=8gb

cd ${PBS_O_WORKDIR}

cd /rds/general/user/lhoxhaj/home/code

module load tools/prod
module load SciPy-bundle/2022.05-foss-2022a

python main_hpc_final.py
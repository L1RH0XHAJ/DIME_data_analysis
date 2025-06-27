#PBS -l walltime=12:00:00
#PBS -l select=1:ncpus=2:mem=2028gb

cd /rds/general/user/lhoxhaj/home/code

module load tools/prod
module load SciPy-bundle/2022.05-foss-2022a
python3 outputs_hpc.py

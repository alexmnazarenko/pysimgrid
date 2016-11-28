SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd "$SCRIPT_DIR/.."

python3 -m pysimgrid.tools.experiment dag/plat_exp1 SyntheticWorkflows/GENOME/GENOME.n.50.1.dax dag/algorithms.json dag/exp1.json

popd

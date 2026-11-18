#!/bin/bash
set -euxo pipefail
sudo python3 -m pip --version || sudo yum install -y python3-pip
sudo python3 -m pip install "numpy==1.26.4"
python3 -c "import numpy; print('numpy', numpy.__version__)"

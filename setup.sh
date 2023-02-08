#!/bin/bash

chmod 777 ./linux/bi-venv/install-bi-venv.sh
chmod 777 *.sh

./linux/bi-venv/install-bi-venv.sh -b -p ./linux/bi-venv/venv/
./linux/bi-venv/venv/bin/conda init bash
source ~/.bashrc
./linux/bi-venv/venv/bin/conda create -n forecast -y python --offline

conda activate forecast
cat ./linux/wheelhouse/split/tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.tar.gz* | tar -zxvpf -
mv tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl ./linux/wheelhouse/
pip3 install --no-index --find-links=linux/wheelhouse/ -r requirements-offline.txt

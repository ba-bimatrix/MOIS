#!/bin/bash

chmod 777 ./linux/bi-venv/install-bi-venv-3.10.sh
chmod 777 *.sh
./linux/bi-venv/install-bi-venv.sh -b -p ./linux/bi-venv/venv/
./linux/bi-venv/venv/bin/conda init bash
source ~/.bashrc
./linux/bi-venv/venv/bin/conda create -n mois3.10 -y python --offline

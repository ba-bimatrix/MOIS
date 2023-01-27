#!/bin/bash
# 가상환경 설치
chmod 777 ./linux/bi-venv/install-bi-venv.sh
chmod 777 ./test.sh
./linux/bi-venv/install-bi-venv.sh -b -p ./linux/bi-venv/venv/
./linux/bi-venv/venv/bin/conda init bash
source ~/.bashrc
./linux/bi-venv/venv/bin/conda create -n mois -y python --offline

# 시작옵션 설정
echo "conda activate mois" >> ~/.bashrc
source ~/.bashrc

#!bin/bash
# 라이브러리 설치
cat ./linux/wheelhouse/3.10/split/tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.tar.gz* | tar -zxvpf -
mv tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl ./linux/wheelhouse/3.10/
pip3 install --no-index --find-links=linux/wheelhouse -r requirements-offline-linux.txt

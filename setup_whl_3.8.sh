#!/bin/bash

cat ./linux/wheelhouse/3.8/split/tensorflow_cpu-2.11.0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.tar.gz* | tar -zxvpf -
mv tensorflow_cpu-2.11.0-cp38-cp38-manylinux_2_17_x86_64.manylinux2014_x86_64.whl ./linux/wheelhouse/3.8/
pip3 install --no-index --find-links=linux/wheelhouse/3.8 -r requirements-offline-3.8.txt


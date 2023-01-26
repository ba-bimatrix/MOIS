# 미니콘다 설치
chmod 777 ./linux/miniconda/miniconda.sh
chmod 777 ./activate.sh
chmod 777 ./test.sh
./linux/miniconda/miniconda.sh -b -p ./linux/miniconda/miniconda/

# 가상환경 생성
. ./linux/miniconda/miniconda/bin/activate
./linux/miniconda/miniconda/bin/conda create -n mois -y
./linux/miniconda/miniconda/bin/conda activate mois

# 분할압축 풀기
cat ./linux/wheelhouse/split/tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl.tar.gz* | tar -zxvpf -
mv tensorflow_cpu-2.11.0-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.whl ./linux/wheelhouse/

# 라이브러리 설치
pip3.10 install --no-index --find-links=linux/wheelhouse -r requirements-offline-linux.txt

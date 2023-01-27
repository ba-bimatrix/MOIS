# MOIS Disaster Material Supply Chain Ananlysis

행정안전부(행안부) 재난물자 공급망 분석 프로젝트 \
Disaster Material Supply Chain Analysis Project for Ministry of the Interior and Safety(MOIS)

[![Py3.10 Build](https://github.com/ba-bimatrix/MOIS/actions/workflows/Py3.10%20Build.yml/badge.svg)](https://github.com/ba-bimatrix/MOIS/actions/workflows/Py3.10%20Build.yml)
[![Py3.8 Build](https://github.com/ba-bimatrix/MOIS/actions/workflows/Py3.8%20Build.yml/badge.svg)](https://github.com/ba-bimatrix/MOIS/actions/workflows/Py3.8%20Build.yml)

<br><br><br>

# 설치방법
- 플랫폼 : 리눅스 (REHL 7.9)

### 1. 가상환경 설치
- 터미널에 설치하고자 하는 버전에 권한 부여 
  <br> [python 3.10] ```chmod 777 ./setup_venv_3.10.sh```
  <br> [python 3.8] ```chmod 777 ./setup_venv_3.8.sh```
  
  
- 터미널에 다음을 입력하여 가상환경 설치를 실행 
  <br> (둘중 하나만 선택하여 설치하는것을 권장)
  <br> [python 3.10] ```./setup_venv_3.10.sh```
  <br> [python 3.8] ```./setup_venv_3.8.sh```
  
### 2. 라이브러리 설치
- 터미널 좌측의 가상환경 생성을 확인 
  <br> [python 3.10] ```(mois3.10)~$```
  <br> [python 3.8] ```(mois3.8)~$```
  
<br><br><br>

# 테스트
- 터미널에 ```./test.sh``` 입력

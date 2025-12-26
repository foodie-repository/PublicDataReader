# KB 부동산 및 공공데이터 수집 도구

## 프로젝트 소개

KB 부동산 데이터, KOSIS 통계 데이터, 공공데이터포털의 아파트 실거래가 정보를 수집하고 분석하는 도구입니다.

## 주요 기능

- **KB 부동산 데이터 수집**: KB 전세가율 등 부동산 시장 데이터 수집
- **KOSIS 주택 착공 실적 수집**: 국토교통부의 주택유형별 주택건설 착공실적 데이터 수집 (2011년~현재)
- **아파트 실거래가 수집**: 공공데이터포털 API를 통한 전국 아파트 매매 실거래가 데이터 수집
- **Google Sheets 연동**: 수집된 데이터를 Google Sheets에 자동으로 저장 및 업데이트
- **데이터 시각화**: Plotly 및 PyGWalker를 사용한 인터랙티브 데이터 분석 및 시각화

## 기술 스택

- Python 3.11+
- pandas: 데이터 분석 및 처리
- PublicDataReader: 공공데이터 및 KB 부동산, KOSIS 데이터 수집
- gspread: Google Sheets API 연동
- pygwalker: 인터랙티브 데이터 시각화
- plotly: 동적 차트 및 그래프 시각화
- matplotlib, seaborn: 정적 차트 및 그래프 시각화
- Jupyter: 노트북 기반 분석 환경

## 설치 방법

이 프로젝트는 [uv](https://github.com/astral-sh/uv)를 사용합니다.

### 1. uv 설치 (설치되어 있지 않은 경우)

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. 프로젝트 설정

```bash
# 저장소 클론 (또는 프로젝트 디렉토리로 이동)
cd PublicDataReader

# 가상환경 생성 및 의존성 설치
uv sync

# 가상환경 활성화
source .venv/bin/activate  # macOS/Linux
# 또는
.venv\Scripts\activate  # Windows
```

## 사용 방법

### 1. 환경변수 설정

`.env.example` 파일을 복사하여 `.env` 파일을 생성합니다:

```bash
cp .env.example .env
```

`.env` 파일을 열어서 실제 값으로 수정합니다:

```bash
# .env 파일 내용
PUBLIC_DATA_SERVICE_KEY=여기에_실제_API_키_입력
GOOGLE_CREDENTIALS_PATH=credentials.json
GOOGLE_SHEET_NAME=전국 아파트 매매 실거래가_누적
LAWD_CODE_FILE=lawd_code.csv
MONTHS_TO_FETCH=202506,202507
```

### 2. Google Sheets API 설정

1. [Google Cloud Console](https://console.cloud.google.com/)에서 프로젝트 생성
2. Google Sheets API 활성화
3. 서비스 계정 생성 후 JSON 키 파일 다운로드
4. 키 파일을 `credentials.json`으로 프로젝트 디렉토리에 저장

### 3. 공공데이터포털 API 키 발급

1. [공공데이터포털](https://www.data.go.kr/) 회원가입
2. '국토교통부 아파트매매 실거래자료' API 신청
3. 발급받은 API 키를 `.env` 파일의 `PUBLIC_DATA_SERVICE_KEY`에 입력

### 4. 실행

#### KOSIS 주택 착공 실적 수집

```bash
# Python 스크립트로 실행 (데이터 수집 및 CSV 저장)
uv run python PublicData/착공.py

# 또는 Jupyter 노트북으로 실행 (시각화 포함)
uv run jupyter notebook PublicData/착공.ipynb
```

**수집 데이터:**

- 기간: 2011년 1월 ~ 현재 (월별 데이터)
- 지역: 전국 17개 시도
- 주택유형: 아파트, 단독, 다가구, 연립, 다세대
  - **참고**: 다가구는 가구수 기준 (다가구에는 동수·가구수가 있지만 집계에 사용한 데이터는 가구수 기준)

**생성되는 파일:**

- `착공실적_시도별_YYYYMMDD.csv`: 원본 데이터 (Long format)
- `착공실적_피벗_전체기간_최종.csv`: 분석용 피벗 데이터

**시각화 (착공.ipynb):**

- 전국 주택유형별 착공 실적 추이 (아파트 vs 비아파트)
- 시도별 착공 실적 비교 (최근 1년, 누적 막대 그래프)
- 특정 시도의 주택유형별 추이 (사용자 입력)
- 주택유형별 시도 비교 (최근 1년)

#### 아파트 실거래가 수집

```bash
# main.py에서 MONTHS_TO_FETCH 수정 후 실행
uv run python main.py
```

#### Jupyter 노트북 실행

```bash
uv run jupyter notebook
```

노트북 파일:

- **KB/**
  - `KB_전세가율.ipynb`: KB 전세가율 데이터 수집 및 분석
- **PublicData/**
  - `착공.ipynb`: KOSIS 주택 착공 실적 데이터 수집 및 시각화
- `Buy.ipynb`: 아파트 매매 데이터 분석
- `분양권+입주권.ipynb`: 분양권 및 입주권 데이터 분석

## 프로젝트 구조

```
PublicDataReader/
├── main.py                       # 아파트 실거래가 수집 메인 스크립트
├── KB/                          # KB 부동산 데이터 수집
│   ├── KB_전세가율.ipynb          # KB 전세가율 분석 노트북
│   └── KB_전세가율.py             # KB 전세가율 수집 스크립트
├── PublicData/                  # 공공데이터 수집
│   ├── 착공.ipynb                # KOSIS 착공 실적 시각화 노트북
│   ├── 착공.py                   # KOSIS 착공 실적 수집 스크립트
│   ├── 착공실적_시도별_YYYYMMDD.csv  # 수집된 원본 데이터
│   └── 착공실적_피벗_전체기간_최종.csv # 분석용 피벗 데이터
├── Data/                        # 수집된 데이터 파일
│   └── KB_전세가율.csv
├── Archive/                     # 아카이브 파일
│   └── 착공_Sample.ipynb
├── Buy.ipynb                    # 아파트 매매 분석 노트북
├── 분양권+입주권.ipynb             # 분양권/입주권 분석 노트북
├── lawd_code.csv                # 법정동 코드 데이터
├── credentials.json             # Google API 인증 파일 (git 제외)
├── pyproject.toml               # 프로젝트 설정 및 의존성
├── uv.lock                      # uv 의존성 잠금 파일
└── README.md                    # 프로젝트 문서
```

## uv 주요 명령어

```bash
# 패키지 추가
uv add <패키지명>

# 패키지 제거
uv remove <패키지명>

# 의존성 동기화
uv sync

# Python 스크립트 실행
uv run python main.py

# Jupyter 실행
uv run jupyter notebook
```

## 데이터 수집 흐름

### KOSIS 주택 착공 실적

1. **KOSIS API 초기화**: PublicDataReader의 Kosis 클래스 사용
2. **데이터 수집**: 2011년~현재까지 전국 17개 시도별 월별 착공 실적 수집
3. **데이터 전처리**: 소계/기타/총계 제외, 시도명 정식 명칭으로 변환
4. **CSV 저장**: 원본 데이터 및 피벗 테이블 형식으로 저장
5. **시각화**: Plotly를 사용한 인터랙티브 차트 생성

### 아파트 실거래가

1. **법정동 코드 로드**: `lawd_code.csv`에서 전국 법정동 코드 읽기
2. **API 호출**: 각 지역별, 월별로 아파트 실거래가 데이터 수집
3. **데이터 전처리**: 중복 제거 및 고유 ID 생성
4. **Google Sheets 업데이트**: 기존 데이터와 비교하여 신규 데이터만 추가

## 주의사항

- `credentials.json` 파일은 절대 공개 저장소에 업로드하지 마세요
- API 호출 제한을 고려하여 적절한 딜레이를 설정했습니다
- 대량의 데이터를 수집할 경우 시간이 오래 걸릴 수 있습니다

## 개발 환경

- Python: 3.11+
- OS: macOS/Linux/Windows
- 패키지 관리: uv

## 라이선스

개인 프로젝트용

## 참고 자료

- [PublicDataReader 문서](https://github.com/WooilJeong/PublicDataReader)
- [uv 공식 문서](https://docs.astral.sh/uv/)
- [공공데이터포털](https://www.data.go.kr/)
- [KOSIS 국가통계포털](https://kosis.kr/)
- [KB 부동산 통계](https://onland.kbstar.com/)
- [Plotly Python 문서](https://plotly.com/python/)

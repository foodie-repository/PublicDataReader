from PublicDataReader import Kbland
import pandas as pd

api = Kbland()
sido_code = [
    "11",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "36",
    "41",
    "43",
    "44",
    "46",
    "47",
    "48",
    "50",
    "51",
    "52",
]

dfs = []
for code in sido_code:
    params = {
        "매물종별구분": "01",
        "지역코드": code,
    }
    try:
        data = api.get_jeonse_price_ratio(**params)
        df = pd.DataFrame(data)
        df["지역코드"] = code  # 혹시 누락된 경우 명시적으로 추가
        dfs.append(df)
    except Exception as e:
        print(f"지역코드 {code} 오류: {e}")

if not dfs:
    raise RuntimeError("API에서 데이터를 얻지 못했습니다.")

result_table = pd.concat(dfs, ignore_index=True)

# '전세가격비율' 컬럼명을 '전세가율'로 변경
if "전세가격비율" in result_table.columns:
    result_table.rename(columns={"전세가격비율": "전세가율"}, inplace=True)

# '전세가율' 값이 퍼센트 문자열이면 숫자로 변환 및 소수점 둘째자리로 반올림
if "전세가율" in result_table.columns:
    result_table["전세가율"] = (
        result_table["전세가율"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
    )
    result_table["전세가율"] = pd.to_numeric(
        result_table["전세가율"], errors="coerce"
    ).round(2)

# '전세가율' 컬럼에서 Null 값이 있는 행 제외
result_table = result_table[result_table["전세가율"].notna()].reset_index(drop=True)

# '날짜' 컬럼을 yyyy-mm 형태로 변환
date_candidates = ["날짜", "기준년월", "기준일", "date", "기준일자"]
date_col = next((c for c in date_candidates if c in result_table.columns), None)
if date_col is not None:
    s = result_table[date_col].astype(str).str.strip()
    parsed = pd.to_datetime(s, errors="coerce", format="%Y%m")
    parsed = parsed.fillna(pd.to_datetime(s, errors="coerce"))
    result_table["날짜"] = parsed.dt.strftime("%Y-%m")
    if date_col != "날짜":
        result_table.drop(columns=[date_col], inplace=True)

# 시도 지역명 제외
exclude_regions = [
    "서울",
    "부산",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산",
    "세종",
    "경기",
    "충북",
    "충남",
    "전남",
    "경북",
    "경남",
    "제주",
    "강원",
    "전북",
]
if "지역명" in result_table.columns:
    result_table = result_table[
        ~result_table["지역명"].isin(exclude_regions)
    ].reset_index(drop=True)

# 시도코드와 시도명 매핑 딕셔너리 생성
sido_map = {
    "11": "서울특별시",
    "26": "부산광역시",
    "27": "대구광역시",
    "28": "인천광역시",
    "29": "광주광역시",
    "30": "대전광역시",
    "31": "울산광역시",
    "36": "세종특별자치시",
    "41": "경기도",
    "43": "충청북도",
    "44": "충청남도",
    "46": "전라남도",
    "47": "경상북도",
    "48": "경상남도",
    "50": "제주특별자치도",
    "51": "강원특별자치도",
    "52": "전북특별자치도",
}

# '시도' 컬럼 생성: 지역코드 앞 두자리로 시도명 매칭
if "지역코드" in result_table.columns:
    result_table["시도"] = result_table["지역코드"].astype(str).str[:2].map(sido_map)

# '지역명' 컬럼명을 '시군구'로 변경
if "지역명" in result_table.columns:
    result_table.rename(columns={"지역명": "시군구"}, inplace=True)

# '지역코드' 컬럼명을 '시도코드'로 변경
if "지역코드" in result_table.columns:
    result_table.rename(columns={"지역코드": "시도코드"}, inplace=True)

# 컬럼 순서 지정
desired_order = ["시도", "시군구", "날짜", "전세가율", "시도코드", "매물종별구분"]
final_cols = [c for c in desired_order if c in result_table.columns]
result_table = result_table[final_cols]

result_table.to_csv("KB_전세가율.csv", index=False)

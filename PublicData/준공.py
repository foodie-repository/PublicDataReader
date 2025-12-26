"""
KOSIS 주택 준공 실적 데이터 수집 스크립트

이 스크립트는 PublicDataReader 라이브러리를 사용하여
국토교통부의 주택유형별 주택건설 준공실적 데이터를 수집합니다.

데이터 특성:
- KOSIS 준공 데이터는 '월계'로 표시되어 있으며, 이는 월별 실적을 의미합니다.
- 착공실적과 마찬가지로 월별 실제 실적 데이터이므로 누적 계산이 필요하지 않습니다.
- 인허가실적(월별 누계)과는 달리 차분 계산을 수행하지 않습니다.

데이터 범위:
- 기간: 2010년 8월 ~ 2025년 12월 (최신월까지)
- 지역: 전국 시도별
- 주택유형: 아파트, 단독, 다가구(가구수 기준), 연립, 다세대
"""

from PublicDataReader import Kosis
import pandas as pd
from datetime import datetime


def collect_construction_data():
    """
    시도별 주택 준공 실적 데이터를 수집하는 함수
    KOSIS API의 40,000셀 제한을 고려하여 지역을 그룹으로 나눠서 수집

    Returns:
        pd.DataFrame: 수집된 준공 실적 데이터프레임
    """

    # KOSIS API 인스턴스 생성
    # API 키는 공개 API 키입니다
    api_key = "M2ZlYmQxMzFlNmMwOGUwODVhMmZjOTIxNmE3ZjAyYTI="
    api = Kosis(api_key)

    print("KOSIS API 인스턴스 생성 완료")

    # 데이터 수집 파라미터 설정
    # orgId: 기관 ID (116 = 국토교통부)
    # tblId: 통계표 ID (DT_MLTM_5373 = 주택유형별 주택건설 준공실적)
    org_id = "116"
    tbl_id = "DT_MLTM_5373"

    # 지역 코드 리스트 (주택유형별 데이터 수집을 위해 각 지역을 개별로 처리)
    region_groups = [
        {"name": "총계", "codes": ["13102766973A.0001"]},
        {"name": "수도권소계", "codes": ["13102766973A.0002"]},
        {"name": "서울", "codes": ["13102766973A.0003"]},
        {"name": "인천", "codes": ["13102766973A.0004"]},
        {"name": "경기", "codes": ["13102766973A.0005"]},
        {"name": "지방소계", "codes": ["13102766973A.0006"]},
        {"name": "기타광역시", "codes": ["13102766973A.0007"]},
        {"name": "부산", "codes": ["13102766973A.0008"]},
        {"name": "대구", "codes": ["13102766973A.0009"]},
        {"name": "광주", "codes": ["13102766973A.0010"]},
        {"name": "대전", "codes": ["13102766973A.0011"]},
        {"name": "울산", "codes": ["13102766973A.0012"]},
        {"name": "기타지방", "codes": ["13102766973A.0013"]},
        {"name": "세종", "codes": ["13102766973A.0014"]},
        {"name": "강원", "codes": ["13102766973A.0015"]},
        {"name": "충북", "codes": ["13102766973A.0016"]},
        {"name": "충남", "codes": ["13102766973A.0017"]},
        {"name": "전북", "codes": ["13102766973A.0018"]},
        {"name": "전남", "codes": ["13102766973A.0019"]},
        {"name": "경북", "codes": ["13102766973A.0020"]},
        {"name": "경남", "codes": ["13102766973A.0021"]},
        {"name": "제주", "codes": ["13102766973A.0022"]},
    ]

    # 기간을 두 구간으로 나누기 (API 제한 대응)
    date_periods = [
        {"start": "201008", "end": "201812"},  # 2010.08-2018년
        {"start": "201901", "end": "202512"},  # 2019-2025년
    ]

    print(f"데이터 수집 시작: 201008 ~ 202512")
    print(f"총 {len(region_groups)}개 지역 x {len(date_periods)}개 기간으로 나누어 수집합니다.\n")

    # 모든 그룹의 데이터를 저장할 리스트
    all_dataframes = []
    total_requests = len(region_groups) * len(date_periods)
    current_request = 0

    try:
        # 각 지역별로 데이터 수집
        for region_idx, group in enumerate(region_groups, 1):
            group_name = group["name"]
            obj_l1 = "+".join(group["codes"])

            # 각 기간별로 데이터 수집
            for period in date_periods:
                current_request += 1
                start_date = period["start"]
                end_date = period["end"]

                print(f"[{current_request}/{total_requests}] {group_name} ({start_date[:4]}-{end_date[:4]}) 수집 중...")

                # KOSIS API를 통해 데이터 수집
                df_group = api.get_data(
                    "통계자료",
                    orgId=org_id,
                    tblId=tbl_id,
                    objL1=obj_l1,       # 시도별 지역
                    objL2="ALL",        # 대분류 (주택유형 B)
                    objL3="ALL",        # 중분류 (주택유형 C)
                    objL4="ALL",        # 소분류 (주택유형 D)
                    itmId="ALL",        # 항목
                    prdSe="M",          # 기간 구분 (M=월별)
                    startPrdDe=start_date,
                    endPrdDe=end_date,
                )

                if df_group is not None and not df_group.empty:
                    # 수치값을 정수형으로 변환
                    df_group = df_group.astype({'수치값': int})
                    all_dataframes.append(df_group)
                    print(f"  ✓ 수집 완료: {len(df_group)}건\n")
                else:
                    print(f"  ✗ 데이터가 없습니다.\n")

        # 모든 데이터프레임을 하나로 합치기
        if all_dataframes:
            df = pd.concat(all_dataframes, ignore_index=True)
            print(f"데이터 수집 완료! 총 {len(df):,}건의 데이터가 수집되었습니다.")
            return df
        else:
            print("수집된 데이터가 없습니다.")
            return None

    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {str(e)}")
        return None


def save_to_csv(df, filename=None):
    """
    데이터프레임을 CSV 파일로 저장하는 함수

    Args:
        df (pd.DataFrame): 저장할 데이터프레임
        filename (str, optional): 저장할 파일명. 지정하지 않으면 자동 생성
    """
    if df is None or df.empty:
        print("저장할 데이터가 없습니다.")
        return

    # 파일명이 지정되지 않은 경우 현재 날짜로 자동 생성
    if filename is None:
        current_date = datetime.now().strftime("%Y%m%d")
        filename = f"준공실적_시도별_{current_date}.csv"

    try:
        # CSV 파일로 저장 (한글 깨짐 방지를 위해 utf-8-sig 인코딩 사용)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

    except Exception as e:
        print(f"파일 저장 중 오류 발생: {str(e)}")


def create_final_pivot_table(df, filename=None):
    """
    최종 분석용 피벗 테이블을 생성하는 함수

    주의: KOSIS 준공 데이터는 '월계'로 표시되어 있으며, 이는 월별 실적을 의미합니다.
    (인허가의 '월별 누계'와 달리 누적 데이터가 아니므로 차분 계산 불필요)

    전체 기간 데이터를 대상으로:
    - 소계/기타/총계 제외
    - 시도명을 정식 명칭으로 변경
    - 불필요한 컬럼 제거
    - Long format으로 변환 (시점, 시도, 주택유형, 개수)

    Args:
        df (pd.DataFrame): 원본 데이터프레임
        filename (str, optional): 저장할 파일명
    """
    if df is None or df.empty:
        print("저장할 데이터가 없습니다.")
        return

    try:
        print("\n=== 최종 피벗 테이블 생성 중 ===")
        print(f"수집 기간: {df['수록시점'].min()} ~ {df['수록시점'].max()}")

        # 소계/기타/총계 제외 (모든 시점)
        df_filtered = df[~df['분류값명1'].str.contains('소계', na=False)].copy()
        df_filtered = df_filtered[~df_filtered['분류값명1'].str.contains('기타', na=False)]
        df_filtered = df_filtered[df_filtered['분류값명1'] != '총계']

        # 피벗 테이블 생성 (시점별, 시도별)
        pivot_df = df_filtered.pivot_table(
            index=['수록시점', '분류값명1'],
            columns='분류값명4',
            values='수치값',
            aggfunc='sum',
            fill_value=0
        ).reset_index()

        # 시점 포맷 변경 (202510 -> 2025.10)
        pivot_df['시점'] = pivot_df['수록시점'].apply(lambda x: f"{str(x)[:4]}.{str(x)[4:]}")
        pivot_df = pivot_df.drop(columns=['수록시점'])

        # 시도명을 정식 명칭으로 변경
        sido_mapping = {
            '서울': '서울특별시',
            '부산': '부산광역시',
            '대구': '대구광역시',
            '인천': '인천광역시',
            '광주': '광주광역시',
            '대전': '대전광역시',
            '울산': '울산광역시',
            '세종': '세종특별자치시',
            '경기': '경기도',
            '충북': '충청북도',
            '충남': '충청남도',
            '전남': '전라남도',
            '경북': '경상북도',
            '경남': '경상남도',
            '제주': '제주특별자치도',
            '강원': '강원특별자치도',
            '전북': '전북특별자치도'
        }
        pivot_df['분류값명1'] = pivot_df['분류값명1'].map(sido_mapping)

        # 컬럼명 변경
        pivot_df = pivot_df.rename(columns={
            '분류값명1': '시도',
            '가구수': '다가구'
        })

        # 불필요한 컬럼 제거
        columns_to_keep = ['시점', '시도', '아파트', '단독', '다가구', '연립', '다세대']
        available_columns = [col for col in columns_to_keep if col in pivot_df.columns]
        pivot_df = pivot_df[available_columns]

        # Long format으로 변환
        df_long = pivot_df.melt(
            id_vars=['시점', '시도'],
            value_vars=['아파트', '단독', '다가구', '연립', '다세대'],
            var_name='주택유형',
            value_name='개수'
        )

        # 파일명 생성
        if filename is None:
            filename = "준공실적_피벗_전체기간_최종.csv"

        # CSV로 저장
        df_long.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"최종 피벗 테이블이 '{filename}' 파일로 저장되었습니다.")
        print(f"- 총 {len(df_long)}개 행 ({len(df_long['시점'].unique())}개 시점 x {len(df_long['시도'].unique())}개 시도 x {len(df_long['주택유형'].unique())}개 주택유형)")

        return df_long

    except Exception as e:
        print(f"최종 피벗 테이블 생성 중 오류 발생: {str(e)}")
        return None


def display_data_info(df):
    """
    수집된 데이터의 기본 정보를 출력하는 함수

    Args:
        df (pd.DataFrame): 분석할 데이터프레임
    """
    if df is None or df.empty:
        print("출력할 데이터가 없습니다.")
        return

    print("\n" + "="*50)
    print("데이터 정보")
    print("="*50)

    # 데이터프레임 기본 정보
    print(f"\n전체 데이터 건수: {len(df):,}건")
    print(f"컬럼 목록: {', '.join(df.columns.tolist())}")

    # 지역별 데이터 건수
    if '분류값명1' in df.columns:
        print("\n[지역별 데이터 건수]")
        region_counts = df['분류값명1'].value_counts()
        for region, count in region_counts.items():
            print(f"  - {region}: {count:,}건")

    # 기간 정보
    if '수록시점' in df.columns:
        print(f"\n[수집 기간]")
        print(f"  - 시작: {df['수록시점'].min()}")
        print(f"  - 종료: {df['수록시점'].max()}")

    # 데이터 샘플 출력
    print("\n[데이터 샘플 (상위 5건)]")
    print(df.head())

    print("\n" + "="*50)


def main():
    """
    메인 실행 함수
    """
    print("="*50)
    print("KOSIS 주택 준공 실적 데이터 수집 시작")
    print("="*50)

    # 1. 데이터 수집
    df = collect_construction_data()

    if df is not None:
        # 2. 데이터 정보 출력
        display_data_info(df)

        # 3. 원본 CSV 파일로 저장 (Long format)
        save_to_csv(df)

        # 4. 최종 분석용 피벗 테이블 생성
        create_final_pivot_table(df)

        print("\n모든 작업이 완료되었습니다!")
    else:
        print("\n데이터 수집에 실패했습니다.")


if __name__ == "__main__":
    # 스크립트가 직접 실행될 때만 main 함수 실행
    main()

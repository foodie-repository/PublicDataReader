"""
KOSIS 주택 인허가 실적 데이터 수집 스크립트

이 스크립트는 PublicDataReader 라이브러리를 사용하여
국토교통부의 주택유형별 주택건설 인허가실적 데이터를 수집합니다.

데이터 범위:
- 기간: 2007년 1월 ~ 2025년 12월 (최신월까지)
- 지역: 전국 시도별
- 주택유형: 아파트, 단독, 다가구(가구수 기준), 연립, 다세대
"""

from PublicDataReader import Kosis
import pandas as pd
from datetime import datetime


def explore_table_metadata():
    """
    통계표의 메타데이터를 확인하는 함수
    """
    api_key = "M2ZlYmQxMzFlNmMwOGUwODVhMmZjOTIxNmE3ZjAyYTI="
    api = Kosis(api_key)

    print("KOSIS API 인스턴스 생성 완료")

    org_id = "116"
    tbl_id = "DT_MLTM_1948"

    print("\n=== 통계표 메타데이터 조회 ===\n")

    try:
        # 1. 분류항목 조회 (블로그 예시 방법)
        print("1. 분류항목 조회")
        result1 = api.get_data(
            "통계표설명",
            "분류항목",
            orgId=org_id,
            tblId=tbl_id
        )

        if result1 is not None and isinstance(result1, pd.DataFrame):
            # 전체 컬럼 확인
            print(f"컬럼 목록: {result1.columns.tolist()}\n")

            # 분류ID별로 그룹화해서 출력
            for class_id in result1['분류ID'].unique():
                df_class = result1[result1['분류ID'] == class_id]
                print(f"\n=== {class_id} (분류값순번: {df_class['분류값순번'].iloc[0]}) ===")
                print(df_class[['분류값ID', '분류값명', '분류값영문명']].to_string())
                print()
        else:
            print(result1)
        print("\n")

        # 2. 통계항목 조회
        print("2. 통계항목 조회")
        result2 = api.get_data(
            "통계표설명",
            "통계항목",
            orgId=org_id,
            tblId=tbl_id
        )
        print(result2)
        print("\n")

        # 3. 통계표 기본정보
        print("3. 통계표 기본정보")
        result3 = api.get_data(
            "통계표설명",
            orgId=org_id,
            tblId=tbl_id
        )
        print(result3)
        print("\n")

        return result1

    except Exception as e:
        print(f"메타데이터 조회 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def collect_permit_data_full():
    """
    시도별 주택 인허가 실적 전체 데이터를 수집하는 함수
    KOSIS API의 40,000셀 제한을 고려하여 지역과 기간을 나눠서 수집

    Returns:
        pd.DataFrame: 수집된 인허가 실적 데이터프레임
    """

    # KOSIS API 인스턴스 생성
    api_key = "M2ZlYmQxMzFlNmMwOGUwODVhMmZjOTIxNmE3ZjAyYTI="
    api = Kosis(api_key)

    print("KOSIS API 인스턴스 생성 완료")

    org_id = "116"
    tbl_id = "DT_MLTM_1948"

    # 지역 코드 리스트
    region_groups = [
        {"name": "전국", "codes": ["13102871090A.0001"]},
        {"name": "수도권", "codes": ["13102871090A.0002"]},
        {"name": "서울", "codes": ["13102871090A.0003"]},
        {"name": "인천", "codes": ["13102871090A.0004"]},
        {"name": "경기", "codes": ["13102871090A.0005"]},
        {"name": "지방소계", "codes": ["13102871090A.0006"]},
        {"name": "기타광역시", "codes": ["13102871090A.0007"]},
        {"name": "부산", "codes": ["13102871090A.0008"]},
        {"name": "대구", "codes": ["13102871090A.0009"]},
        {"name": "광주", "codes": ["13102871090A.0010"]},
        {"name": "대전", "codes": ["13102871090A.0011"]},
        {"name": "울산", "codes": ["13102871090A.0012"]},
        {"name": "기타지방", "codes": ["13102871090A.0013"]},
        {"name": "강원", "codes": ["13102871090A.0014"]},
        {"name": "충북", "codes": ["13102871090A.0015"]},
        {"name": "충남", "codes": ["13102871090A.0016"]},
        {"name": "전북", "codes": ["13102871090A.0017"]},
        {"name": "전남", "codes": ["13102871090A.0018"]},
        {"name": "경북", "codes": ["13102871090A.0019"]},
        {"name": "경남", "codes": ["13102871090A.0020"]},
        {"name": "제주", "codes": ["13102871090A.0021"]},
        {"name": "세종", "codes": ["13102871090A.0022"]},
    ]

    # 기간을 여러 구간으로 나누기 (API 제한 대응)
    date_periods = [
        {"start": "200701", "end": "201212"},  # 2007-2012년
        {"start": "201301", "end": "201812"},  # 2013-2018년
        {"start": "201901", "end": "202512"},  # 2019-2025년
    ]

    print(f"데이터 수집 시작: 200701 ~ 202512")
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
                    objL3="ALL",        # 중분류 (주택유형 C - 다가구 포함)
                    objL4="ALL",        # 소분류 (주택유형 D - 동수/가구수)
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
        import traceback
        traceback.print_exc()
        return None


def collect_permit_data_test():
    """
    시도별 주택 인허가 실적 데이터를 수집하는 함수
    KOSIS API의 40,000셀 제한을 고려하여 지역을 그룹으로 나눠서 수집

    Returns:
        pd.DataFrame: 수집된 인허가 실적 데이터프레임
    """

    # KOSIS API 인스턴스 생성
    # API 키는 공개 API 키입니다
    api_key = "M2ZlYmQxMzFlNmMwOGUwODVhMmZjOTIxNmE3ZjAyYTI="
    api = Kosis(api_key)

    print("KOSIS API 인스턴스 생성 완료")

    # 데이터 수집 파라미터 설정
    # orgId: 기관 ID (116 = 국토교통부)
    # tblId: 통계표 ID (DT_MLTM_1948 = 주택유형별 주택건설 인허가실적)
    org_id = "116"
    tbl_id = "DT_MLTM_1948"

    # 먼저 메타데이터를 확인하기 위해 매우 작은 범위로 테스트
    print("\n=== 테스트: 데이터 수집 (최소 범위) ===")
    print("분류 코드를 확인하기 위한 테스트입니다.\n")

    try:
        # 테스트 데이터 수집 - 인허가 테이블의 실제 분류값ID 사용
        # 서울만 1개월 데이터로 테스트

        # objL2: 주택유형 B (합계, 단독, 다세대, 연립, 아파트)
        housing_types_b = "+".join([
            "13102871090B.0002",  # 합계(가구수기준)
            "13102871090B.0003",  # 단독
            "13102871090B.0004",  # 다세대
            "13102871090B.0005",  # 연립
            "13102871090B.0006",  # 아파트
        ])

        # objL3: 주택유형 C (다가구 포함)
        housing_types_c = "+".join([
            "13102871090C.0002",  # 합계(가구수기준)
            "13102871090C.0003",  # 단독
            "13102871090C.0004",  # 다가구
            "13102871090C.0005",  # 다세대
            "13102871090C.0006",  # 연립
            "13102871090C.0007",  # 아파트
        ])

        # objL4: 주택유형 D (동수/가구수 구분)
        housing_types_d = "+".join([
            "13102871090D.0002",  # 합계(가구수기준)
            "13102871090D.0003",  # 단독
            "13102871090D.0006",  # 가구수
            "13102871090D.0007",  # 다세대
            "13102871090D.0008",  # 연립
            "13102871090D.0009",  # 아파트
        ])

        df_test = api.get_data(
            "통계자료",
            orgId=org_id,
            tblId=tbl_id,
            objL1="13102871090A.0003",  # 서울
            objL2="ALL",                # 주택유형 B
            objL3="ALL",                # 주택유형 C
            objL4="ALL",                # 주택유형 D
            itmId="ALL",                # 항목
            prdSe="M",                  # 기간 구분 (M=월별)
            startPrdDe="202412",        # 2024년 12월만
            endPrdDe="202412",          # 2024년 12월만
        )

        if df_test is not None and not df_test.empty:
            print(f"✓ 테스트 데이터 수집 완료: {len(df_test)}건\n")
            print("=" * 50)
            print("데이터 구조 확인")
            print("=" * 50)
            print(f"\n컬럼 목록:\n{df_test.columns.tolist()}\n")

            # 분류값 확인
            if '분류값명1' in df_test.columns:
                print(f"[분류값명1] 고유값 (시도):\n{df_test['분류값명1'].unique()}\n")

            if '분류값명2' in df_test.columns:
                print(f"[분류값명2] 고유값:\n{df_test['분류값명2'].unique()}\n")

            if '분류값명3' in df_test.columns:
                print(f"[분류값명3] 고유값:\n{df_test['분류값명3'].unique()}\n")

            if '분류값명4' in df_test.columns:
                print(f"[분류값명4] 고유값 (주택유형):\n{df_test['분류값명4'].unique()}\n")

            print("=" * 50)
            print("\n[샘플 데이터]")
            print(df_test.head(10))
            print("\n")

            return df_test
        else:
            print("✗ 테스트 데이터 수집 실패\n")
            return None

    except Exception as e:
        print(f"데이터 수집 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
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
        filename = f"인허가실적_시도별_{current_date}.csv"

    try:
        # CSV 파일로 저장 (한글 깨짐 방지를 위해 utf-8-sig 인코딩 사용)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

    except Exception as e:
        print(f"파일 저장 중 오류 발생: {str(e)}")


def create_final_pivot_table(df, filename=None):
    """
    최종 분석용 피벗 테이블을 생성하는 함수

    중요: KOSIS 인허가 데이터는 '월별 누계'이므로 각 월의 실제 실적을 구하기 위해
    차분(difference) 계산을 수행합니다.

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

        # 소계/기타/전국/수도권 제외 (실제 시도만 포함)
        df_filtered = df[~df['분류값명1'].str.contains('소계', na=False)].copy()
        df_filtered = df_filtered[~df_filtered['분류값명1'].str.contains('기타', na=False)]
        df_filtered = df_filtered[df_filtered['분류값명1'] != '전국']
        df_filtered = df_filtered[df_filtered['분류값명1'] != '수도권']

        # 가구수 기준 데이터만 필터링
        housing_types_to_keep = ['단독', '가구수', '다세대', '연립', '아파트']
        df_filtered = df_filtered[df_filtered['분류값명4'].isin(housing_types_to_keep)]

        # 시점 포맷 변경 (202510 -> 2025.10)
        df_filtered['시점'] = df_filtered['수록시점'].apply(lambda x: f"{str(x)[:4]}.{str(x)[4:]}")
        df_filtered['연도'] = df_filtered['수록시점'].apply(lambda x: str(x)[:4])
        df_filtered['월'] = df_filtered['수록시점'].apply(lambda x: int(str(x)[4:]))

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
        df_filtered['시도'] = df_filtered['분류값명1'].map(sido_mapping)

        # 주택유형명 변경
        df_filtered['주택유형'] = df_filtered['분류값명4'].replace({'가구수': '다가구'})

        # 필요한 컬럼만 선택
        df_work = df_filtered[['시점', '시도', '주택유형', '수치값', '연도', '월']].copy()

        # 시점 순으로 정렬
        df_work = df_work.sort_values(['시도', '주택유형', '시점'])

        # ★ 월별 차분 계산 (누적 -> 실제 월별 실적)
        print("\n누적 데이터를 월별 실적으로 변환 중...")

        # 시도별, 주택유형별로 그룹화하여 차분 계산
        df_work['실적'] = df_work.groupby(['시도', '주택유형'])['수치값'].diff()

        # 각 연도의 첫 달(1월)은 diff가 NaN이 되므로, 원래 값 사용
        df_work.loc[df_work['월'] == 1, '실적'] = df_work.loc[df_work['월'] == 1, '수치값']

        # 음수 값 처리 (연도가 바뀔 때 발생 가능)
        df_work.loc[df_work['실적'] < 0, '실적'] = df_work.loc[df_work['실적'] < 0, '수치값']

        # 정수로 변환
        df_work['실적'] = df_work['실적'].fillna(0).astype(int)

        # 최종 데이터프레임 생성
        df_long = df_work[['시점', '시도', '주택유형', '실적']].copy()
        df_long = df_long.rename(columns={'실적': '개수'})

        # 파일명 생성
        if filename is None:
            filename = "인허가실적_피벗_전체기간_최종.csv"

        # CSV로 저장
        df_long.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✓ 최종 피벗 테이블이 '{filename}' 파일로 저장되었습니다.")
        print(f"- 총 {len(df_long):,}개 행")
        print(f"- {len(df_long['시점'].unique())}개 시점 x {len(df_long['시도'].unique())}개 시도 x {len(df_long['주택유형'].unique())}개 주택유형")
        print(f"- 누적 데이터 → 월별 실적으로 변환 완료")

        return df_long

    except Exception as e:
        print(f"최종 피벗 테이블 생성 중 오류 발생: {str(e)}")
        import traceback
        traceback.print_exc()
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
    print("KOSIS 주택 인허가 실적 데이터 수집 시작")
    print("="*50)

    # 전체 데이터 수집
    df = collect_permit_data_full()

    if df is not None:
        # 데이터 정보 출력
        display_data_info(df)

        # 원본 CSV 파일로 저장
        save_to_csv(df)

        # 최종 분석용 피벗 테이블 생성
        create_final_pivot_table(df)

        print("\n모든 작업이 완료되었습니다!")
    else:
        print("\n데이터 수집에 실패했습니다.")


if __name__ == "__main__":
    # 스크립트가 직접 실행될 때만 main 함수 실행
    main()

"""
KOSIS 아파트 미분양 현황 데이터 수집 스크립트

이 스크립트는 PublicDataReader 라이브러리를 사용하여
국토교통부의 시군구별 아파트 미분양 현황 데이터를 수집합니다.

데이터 범위:
- 기간: 2007년 1월 ~ 2025년 12월 (최신월까지)
- 지역: 전국 시도별 + 시군구별
- 주택유형: 아파트 미분양 (주택유형 구분 없음)
"""

from PublicDataReader import Kosis
import pandas as pd
from datetime import datetime


def collect_unsold_data():
    """
    시군구별 아파트 미분양 현황 데이터를 수집하는 함수
    KOSIS API의 40,000셀 제한을 고려하여 지역별로 나눠서 수집

    Returns:
        pd.DataFrame: 수집된 미분양 현황 데이터프레임
    """

    # KOSIS API 인스턴스 생성
    # API 키는 공개 API 키입니다
    api_key = "M2ZlYmQxMzFlNmMwOGUwODVhMmZjOTIxNmE3ZjAyYTI="
    api = Kosis(api_key)

    print("KOSIS API 인스턴스 생성 완료")

    # 데이터 수집 파라미터 설정
    # orgId: 기관 ID (101 = 국토교통부)
    # tblId: 통계표 ID (DT_1YL202004E = 미분양주택현황(시도/시/군/구))
    org_id = "101"
    tbl_id = "DT_1YL202004E"

    # 지역 코드 리스트 (17개 시도)
    region_groups = [
        {"name": "서울", "codes": ["13102871087A.0002"]},
        {"name": "부산", "codes": ["13102871087A.0003"]},
        {"name": "대구", "codes": ["13102871087A.0004"]},
        {"name": "인천", "codes": ["13102871087A.0005"]},
        {"name": "광주", "codes": ["13102871087A.0006"]},
        {"name": "대전", "codes": ["13102871087A.0007"]},
        {"name": "울산", "codes": ["13102871087A.0008"]},
        {"name": "경기", "codes": ["13102871087A.0009"]},
        {"name": "강원", "codes": ["13102871087A.0010"]},
        {"name": "충북", "codes": ["13102871087A.0011"]},
        {"name": "충남", "codes": ["13102871087A.0012"]},
        {"name": "전북", "codes": ["13102871087A.0013"]},
        {"name": "전남", "codes": ["13102871087A.0014"]},
        {"name": "경북", "codes": ["13102871087A.0015"]},
        {"name": "경남", "codes": ["13102871087A.0016"]},
        {"name": "제주", "codes": ["13102871087A.0017"]},
        {"name": "세종", "codes": ["13102871087A.0018"]},
    ]

    # 기간을 세 구간으로 나누기 (API 제한 대응)
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
                    objL1=obj_l1,      # 시도별 지역
                    objL2="ALL",       # 시군구별 (계 포함)
                    itmId="ALL",       # 항목 (미분양현황)
                    prdSe="M",         # 기간 구분 (M=월별)
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
        filename = f"미분양현황_시군구별_{current_date}.csv"

    try:
        # CSV 파일로 저장 (한글 깨짐 방지를 위해 utf-8-sig 인코딩 사용)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"데이터가 '{filename}' 파일로 저장되었습니다.")

    except Exception as e:
        print(f"파일 저장 중 오류 발생: {str(e)}")


def create_final_pivot_table(df, filename=None):
    """
    최종 분석용 피벗 테이블을 생성하는 함수

    전체 기간 데이터를 대상으로:
    - 시군구별 데이터 유지
    - 시도명을 정식 명칭으로 변경
    - Long format으로 변환 (시점, 시도, 시군구, 미분양수)

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

        # 데이터 복사
        df_work = df.copy()

        # 시점 포맷 변경 (202510 -> 2025.10, 월은 항상 2자리로)
        df_work['시점'] = df_work['수록시점'].apply(lambda x: f"{str(x)[:4]}.{str(x)[4:].zfill(2)}")

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
        df_work['시도'] = df_work['분류값명1'].map(sido_mapping)

        # 필요한 컬럼만 선택
        df_final = df_work[['시점', '시도', '분류값명2', '수치값']].copy()
        df_final = df_final.rename(columns={
            '분류값명2': '시군구',
            '수치값': '미분양수'
        })

        # '계' (합계) 행 제외 - 시군구별 세부 데이터만 유지
        df_final = df_final[df_final['시군구'] != '계'].copy()

        # 파일명 생성
        if filename is None:
            filename = "미분양현황_피벗_전체기간_최종.csv"

        # CSV로 저장
        df_final.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"최종 피벗 테이블이 '{filename}' 파일로 저장되었습니다.")
        print(f"- 총 {len(df_final):,}개 행 ({len(df_final['시점'].unique())}개 시점 x {len(df_final['시도'].unique())}개 시도 x 시군구별)")

        return df_final

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

    # 시군구 개수
    if '분류값명2' in df.columns:
        print(f"\n[시군구 개수]")
        print(f"  - 전체: {len(df['분류값명2'].unique())}개")

    # 데이터 샘플 출력
    print("\n[데이터 샘플 (상위 5건)]")
    print(df.head())

    print("\n" + "="*50)


def main():
    """
    메인 실행 함수
    """
    print("="*50)
    print("KOSIS 아파트 미분양 현황 데이터 수집 시작")
    print("="*50)

    # 1. 데이터 수집
    df = collect_unsold_data()

    if df is not None:
        # 2. 데이터 정보 출력
        display_data_info(df)

        # 3. 원본 CSV 파일로 저장
        save_to_csv(df)

        # 4. 최종 분석용 피벗 테이블 생성
        create_final_pivot_table(df)

        print("\n모든 작업이 완료되었습니다!")
    else:
        print("\n데이터 수집에 실패했습니다.")


if __name__ == "__main__":
    # 스크립트가 직접 실행될 때만 main 함수 실행
    main()

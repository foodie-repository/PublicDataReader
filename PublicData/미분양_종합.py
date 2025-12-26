"""
미분양 + 준공 후 미분양 데이터 통합 스크립트

미분양현황_피벗_전체기간_최종.csv와 준공_후_미분양_피벗_전체기간_최종.csv를
합쳐서 미분양_종합.csv 파일을 생성합니다.
"""

import pandas as pd
from datetime import datetime


def merge_unsold_data():
    """
    미분양과 준공 후 미분양 데이터를 합치는 함수

    Returns:
        pd.DataFrame: 통합된 데이터프레임
    """
    print("=" * 50)
    print("미분양 데이터 통합 시작")
    print("=" * 50)

    # 1. 미분양 데이터 로드
    print("\n1. 미분양 데이터 로드 중...")
    df_unsold = pd.read_csv('미분양현황_피벗_전체기간_최종.csv', encoding='utf-8-sig', dtype={'시점': str})
    print(f"   - 미분양 데이터: {len(df_unsold):,}건")
    print(f"   - 기간: {df_unsold['시점'].min()} ~ {df_unsold['시점'].max()}")

    # 2. 준공 후 미분양 데이터 로드
    print("\n2. 준공 후 미분양 데이터 로드 중...")
    df_completed = pd.read_csv('준공_후_미분양_피벗_전체기간_최종.csv', encoding='utf-8-sig', dtype={'시점': str})
    print(f"   - 준공 후 미분양 데이터: {len(df_completed):,}건")
    print(f"   - 기간: {df_completed['시점'].min()} ~ {df_completed['시점'].max()}")

    # 3. 준공 후 미분양 데이터의 컬럼명 변경
    df_completed = df_completed.rename(columns={'미분양수': '준공_후_미분양수'})

    # 4. 데이터 병합 (미분양 기준 left join)
    print("\n3. 데이터 병합 중...")
    df_merged = pd.merge(
        df_unsold,
        df_completed[['시점', '시도', '시군구', '준공_후_미분양수']],
        on=['시점', '시도', '시군구'],
        how='left'
    )

    # 5. 준공 후 미분양이 없는 경우 0으로 채우기
    df_merged['준공_후_미분양수'] = df_merged['준공_후_미분양수'].fillna(0).astype(int)

    # 6. 준공 전 미분양수 계산 (미분양수 - 준공_후_미분양수)
    df_merged['준공_전_미분양수'] = df_merged['미분양수'] - df_merged['준공_후_미분양수']

    # 7. 컬럼 순서 정리
    df_merged = df_merged[['시점', '시도', '시군구', '미분양수', '준공_후_미분양수', '준공_전_미분양수']]

    print(f"   - 통합 데이터: {len(df_merged):,}건")
    print(f"   - 기간: {df_merged['시점'].min()} ~ {df_merged['시점'].max()}")

    # 7. 통계 출력
    print("\n=== 통합 데이터 통계 ===")
    print(f"총 행 수: {len(df_merged):,}")
    print(f"시점 범위: {df_merged['시점'].min()} ~ {df_merged['시점'].max()}")
    print(f"시도 개수: {len(df_merged['시도'].unique())}개")
    print(f"시군구 개수: {len(df_merged['시군구'].unique())}개")

    # 8. 미리보기
    print("\n=== 데이터 미리보기 (상위 20건) ===")
    print(df_merged.head(20).to_string(index=True))

    # 9. 준공 후 미분양이 있는 데이터 확인
    has_completed = df_merged[df_merged['준공_후_미분양수'] > 0]
    print(f"\n준공 후 미분양 데이터가 있는 행: {len(has_completed):,}건")

    return df_merged


def save_merged_data(df, filename='미분양_종합.csv'):
    """
    통합 데이터를 CSV 파일로 저장

    Args:
        df (pd.DataFrame): 저장할 데이터프레임
        filename (str): 저장할 파일명
    """
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"\n통합 데이터가 '{filename}' 파일로 저장되었습니다.")
        print(f"파일 크기: {len(df):,}행 × {len(df.columns)}열")
        print(f"컬럼: {', '.join(df.columns.tolist())}")
    except Exception as e:
        print(f"\n파일 저장 중 오류 발생: {str(e)}")


def main():
    """
    메인 실행 함수
    """
    # 데이터 병합
    df_merged = merge_unsold_data()

    # 파일 저장
    save_merged_data(df_merged)

    print("\n모든 작업이 완료되었습니다!")


if __name__ == "__main__":
    main()

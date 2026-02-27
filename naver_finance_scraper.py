# -*- coding: utf-8 -*-
"""
[Windows 설치/실행 가이드]
1) 실행 불가 원인 진단 (CMD 또는 PowerShell)
   - py --version
   - python --version
   - py -m pip --version
   - py -m pip list
   - py -c "import sys; print(sys.executable)"
   - py -c "import pandas, selenium; print('OK')"
   - py -c "from selenium import webdriver; d=webdriver.Chrome(); d.get('https://www.naver.com'); print(d.title); d.quit()"

2) 필수 패키지 설치
   - py -m pip install --upgrade pip
   - py -m pip install selenium pandas lxml html5lib

3) 실행
   - py naver_finance_scraper.py

[설명]
- Selenium 4.x + Selenium Manager 기본 동작으로 ChromeDriver를 자동 관리합니다.
- 실패 시 자동으로 webdriver-manager 대안을 시도합니다.
- 네이버 금융 시세/시장종합에서 fieldIds 체크박스로 지표를 선택하고 페이지별 테이블을 읽어 CSV로 저장합니다.
"""

from __future__ import annotations

import os
import traceback
from io import StringIO
from typing import List, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


OUTPUT_FILE = "kospi_kosdaq_companies.csv"
WANTED_FIELDS = ["시가총액", "PER", "ROE", "PBR", "매출액증가율", "유보율"]
BASE_URLS = {
    "코스닥": "https://finance.naver.com/sise/sise_market_sum.naver?sosok=1&page=",
    "코스피": "https://finance.naver.com/sise/sise_market_sum.naver?&page=",
}
MAX_PAGES = {
    "코스닥": 40,
    "코스피": 50,
}
MAX_RETRY = 3
WAIT_SEC = 15


def create_driver() -> webdriver.Chrome:
    """Selenium Manager 우선, 실패 시 webdriver-manager 대안 시도."""
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR")

    try:
        print("[INFO] Chrome WebDriver 시작 (Selenium Manager 자동 드라이버 관리)...")
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        return driver
    except Exception as e:
        print(f"[WARN] Selenium Manager 방식 실패: {e}")
        print("[INFO] webdriver-manager 대안 시도...")
        try:
            from webdriver_manager.chrome import ChromeDriverManager

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.maximize_window()
            return driver
        except Exception as inner:
            print("[ERROR] WebDriver 초기화 실패")
            print(traceback.format_exc())
            raise RuntimeError(
                "Chrome 실행에 실패했습니다. 크롬 설치 여부/버전/보안 프로그램을 확인하세요."
            ) from inner



def wait_for_market_table_ready(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.type_2"))
    )



def configure_fields(driver: webdriver.Chrome, market_first_page_url: str) -> None:
    """체크박스(fieldIds)에서 원하는 지표만 선택 후 적용."""
    driver.get(market_first_page_url)
    wait_for_market_table_ready(driver)

    check_boxes = WebDriverWait(driver, WAIT_SEC).until(
        EC.presence_of_all_elements_located((By.NAME, "fieldIds"))
    )

    # 기존 선택 해제
    for cb in check_boxes:
        if cb.is_selected():
            cb.click()

    # 원하는 항목 선택
    for cb in check_boxes:
        parent = cb.find_element(By.XPATH, "..")
        label = parent.find_element(By.TAG_NAME, "label")
        if label.text.strip() in WANTED_FIELDS:
            cb.click()

    btn_apply = WebDriverWait(driver, WAIT_SEC).until(
        EC.element_to_be_clickable((By.XPATH, '//a[@href="javascript:fieldSubmit()"]'))
    )
    btn_apply.click()
    wait_for_market_table_ready(driver)
    print("[INFO] 지표 선택/적용 완료")



def parse_stock_table_from_html(html: str) -> Optional[pd.DataFrame]:
    """테이블 인덱스 고정 없이 '종목명' 컬럼이 있는 테이블을 찾아 반환."""
    tables = pd.read_html(StringIO(html))
    for table in tables:
        table = table.dropna(axis="index", how="all").dropna(axis="columns", how="all")

        # 멀티헤더 대비
        if isinstance(table.columns, pd.MultiIndex):
            table.columns = [
                " ".join([str(x) for x in col if str(x) != "nan"]).strip()
                for col in table.columns
            ]
        else:
            table.columns = [str(c).strip() for c in table.columns]

        if "종목명" in table.columns:
            return table
    return None



def append_csv_safely(df: pd.DataFrame, file_name: str) -> None:
    """최초 1회 헤더, 이후 append로 안전 저장."""
    write_header = not os.path.exists(file_name)
    df.to_csv(
        file_name,
        encoding="utf-8-sig",
        index=False,
        mode="a" if not write_header else "w",
        header=write_header,
    )



def collect_market(driver: webdriver.Chrome, market_name: str, base_url: str, max_pages: int) -> None:
    for page in range(1, max_pages + 1):
        url = f"{base_url}{page}"

        page_success = False
        for attempt in range(1, MAX_RETRY + 1):
            try:
                print(f"[INFO] {market_name} {page}페이지 수집 시도 {attempt}/{MAX_RETRY}")
                driver.get(url)
                wait_for_market_table_ready(driver)

                df = parse_stock_table_from_html(driver.page_source)
                if df is None:
                    print(f"[WARN] {market_name} {page}페이지: '종목명' 테이블을 찾지 못해 스킵")
                    page_success = True  # 구조 변경 시 다음 페이지는 계속 진행
                    break

                if df.empty:
                    print(f"[INFO] {market_name} {page}페이지: 데이터 없음 -> 시장 수집 종료")
                    return

                # 시장 컬럼 추가
                df.insert(0, "시장", market_name)

                append_csv_safely(df, OUTPUT_FILE)
                print(f"[DONE] {market_name} {page}페이지 저장 완료 (rows={len(df)})")
                page_success = True
                break

            except (TimeoutException, WebDriverException, ValueError) as e:
                print(f"[WARN] {market_name} {page}페이지 오류: {e}")
                if attempt == MAX_RETRY:
                    print(f"[SKIP] {market_name} {page}페이지: 재시도 초과로 스킵")

        if not page_success:
            print(f"[SKIP] {market_name} {page}페이지 최종 스킵")



def main() -> None:
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        print(f"[INFO] 기존 파일 삭제: {OUTPUT_FILE}")

    driver = create_driver()
    try:
        # 기존 코드 흐름 유지: 코스닥에서 지표 체크 후 적용
        configure_fields(driver, BASE_URLS["코스닥"] + "1")

        # 코스닥 -> 코스피 순으로 수집
        collect_market(driver, "코스닥", BASE_URLS["코스닥"], MAX_PAGES["코스닥"])
        collect_market(driver, "코스피", BASE_URLS["코스피"], MAX_PAGES["코스피"])

        print(f"[COMPLETE] 수집 완료: {OUTPUT_FILE}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()

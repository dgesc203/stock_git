import os
from supabase import create_client
from datetime import datetime
import pandas as pd
from config.config import SUPABASE_URL, SUPABASE_KEY

# Supabase 클라이언트 초기화
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def create_tables():
    """
    데이터베이스 테이블 생성 (이미 존재하는 경우 무시)
    """
    try:
        with open('schema.sql', 'r', encoding='utf-8') as sql_file:
            sql_commands = sql_file.read()
            
        # Supabase SQL 실행
        # Supabase는 SQL API를 통해 스키마를 직접 생성해야 함
        print("스키마 SQL 파일이 준비되었습니다.")
        print("Supabase 대시보드에서 SQL 에디터를 통해 schema.sql 파일의 내용을 실행해주세요.")
        return True
    except Exception as e:
        print(f"테이블 생성 중 오류 발생: {e}")
        return False

def save_stock_data(stocks_data, table_name="kospi_stocks"):
    """
    주식 데이터를 Supabase에 저장
    
    Args:
        stocks_data: 저장할 주식 데이터 리스트
        table_name: 저장할 테이블 이름 (기본값: kospi_stocks)
    
    Returns:
        성공 여부 (bool)
    """
    try:
        # 데이터 형식 확인 및 변환
        today = datetime.now().strftime('%Y-%m-%d')
        
        if not stocks_data:
            print("저장할 데이터가 없습니다.")
            return False
        
        # 데이터 저장
        for stock in stocks_data:
            if isinstance(stock, dict):
                # 이미 딕셔너리 형태인 경우
                stock_data = stock
                if 'date' not in stock_data:
                    stock_data['date'] = today
            else:
                # 코드만 있는 경우 (급등주 탐지)
                code = stock
                name = ""  # 실제로는 종목명 가져오는 로직 필요
                stock_data = {
                    'date': today,
                    'code': code,
                    'name': name,
                    'price': 0,  # 실제 값으로 수정 필요
                    'change_rate': 0  # 실제 값으로 수정 필요
                }
            
            # Supabase에 데이터 저장
            result = supabase.table(table_name).insert(stock_data).execute()
            
            if hasattr(result, 'error') and result.error:
                print(f"데이터 저장 중 오류 발생: {result.error}")
                return False
        
        print(f"{len(stocks_data)}개의 종목 데이터가 성공적으로 저장되었습니다.")
        return True
        
    except Exception as e:
        print(f"데이터 저장 중 오류 발생: {e}")
        return False

def get_stock_data(table_name="kospi_stocks", limit=100):
    """
    Supabase에서 주식 데이터 조회
    
    Args:
        table_name: 조회할 테이블 이름 (기본값: kospi_stocks)
        limit: 조회할 최대 레코드 수 (기본값: 100)
    
    Returns:
        조회된 데이터 (list)
    """
    try:
        result = supabase.table(table_name).select("*").limit(limit).execute()
        
        if hasattr(result, 'error') and result.error:
            print(f"데이터 조회 중 오류 발생: {result.error}")
            return []
        
        return result.data
    except Exception as e:
        print(f"데이터 조회 중 오류 발생: {e}")
        return [] 
import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta
from collections import Counter
from tqdm import tqdm
import time
from multiprocessing import Pool, cpu_count
import pytz
import os
import logging
import sys
import traceback
import requests

from utils.telegram_service import send_telegram_message
from utils.database import save_stock_data
from config.config import KOSPI_EXECUTION_TIME

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# 파일 핸들러
file_handler = logging.FileHandler('potential_stock_finder.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# 콘솔 핸들러
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# 글로벌 변수
start_date = None
end_date = None

def safe_stock_api_call(func, *args, retries=5, delay=3, **kwargs):
    """
    KRX API 호출을 안전하게 수행하는 헬퍼 함수
    
    Args:
        func: 호출할 pykrx 함수
        retries: 재시도 횟수
        delay: 재시도 간 대기 시간(초)
        *args, **kwargs: 함수에 전달할 인자들
    
    Returns:
        함수 실행 결과 또는 실패 시 None
    """
    for attempt in range(retries):
        try:
            result = func(*args, **kwargs)
            # 결과 유효성 검사
            if result is not None:
                return result
            else:
                logger.warning(f"API 호출 결과가 None입니다 ({func.__name__}), {attempt+1}/{retries} 재시도 중...")
                time.sleep(delay)
        except requests.exceptions.JSONDecodeError as e:
            logger.warning(f"JSON 디코딩 오류 ({func.__name__}): {str(e)}, {attempt+1}/{retries} 재시도 중...")
            time.sleep(delay * 2)  # JSON 오류는 서버 부하 가능성이 높으므로 대기 시간 증가
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"API 호출 실패 ({func.__name__}): {str(e)}, {attempt+1}/{retries} 재시도 중...")
                time.sleep(delay)
            else:
                error_trace = traceback.format_exc()
                logger.error(f"API 호출 최종 실패 ({func.__name__}): {str(e)}\n{error_trace}")
                return None
    
    # 모든 재시도 실패
    logger.error(f"최대 재시도 횟수 초과 ({func.__name__}): 데이터를 가져올 수 없습니다")
    return None

def get_first_workday_of_month(year, month):
    """해당 월의 첫 영업일 구하기"""
    date = pd.date_range(f"{year}-{month}-01", f"{year}-{month}-07", freq='B')[0]
    return date.day

def check_volume_spike(df):
    """거래량 급증 여부 확인"""
    try:
        avg_vol_10 = df['거래량'].tail(11)[:-1].mean()
        current_vol = df['거래량'].iloc[-1]
        result = current_vol > avg_vol_10 * 2
        print(f"  거래량 급증: avg_vol_10={avg_vol_10:.0f}, current_vol={current_vol:.0f}, result={result}")
        return result
    except Exception as e:
        print(f"거래량 급등 분석 실패: {e}")
        return False

def check_market_cap(marcap):
    """시가총액 조건 확인 (5000억 이상)"""
    try:
        result = marcap >= 500000000000
        print(f"  시가총액: marcap={marcap:.0f}, result={result}")
        return result
    except Exception as e:
        print(f"시총 분석 실패: {e}")
        return False

def check_close_to_ma240(df):
    """종가가 240일 이동평균선 근처에 있는지 확인"""
    try:
        if len(df) < 240:
            print(f"  MA240 체크: 데이터 부족으로 계산 불가 (len={len(df)})")
            return False
        ma240 = df['종가'].rolling(240).mean()
        current_close = df['종가'].iloc[-1]
        ma240_value = ma240.iloc[-1]
        result = abs(current_close - ma240_value) / ma240_value < 0.10
        print(f"  MA240 체크: close={current_close:.0f}, ma240={ma240_value:.0f}, result={result}")
        return result
    except Exception as e:
        print(f"MA240 분석 실패: {e}")
        return False

def check_ma_transition(df):
    """이동평균선 전환 확인 (20일선이 60일선 위로)"""
    try:
        if len(df) < 60:
            print(f"  MA Transition: 데이터 부족으로 계산 불가 (len={len(df)})")
            return False
        ma20 = df['종가'].rolling(20).mean()
        ma60 = df['종가'].rolling(60).mean()
        ma20_recent = ma20.tail(5)
        ma60_recent = ma60.tail(5)
        crossover = (ma20_recent.iloc[-2] < ma60_recent.iloc[-2]) and (ma20_recent.iloc[-1] > ma60_recent.iloc[-1])
        result = crossover or (ma20.iloc[-1] > ma60.iloc[-1])
        print(f"  MA Transition: ma20={ma20.iloc[-1]:.0f}, ma60={ma60.iloc[-1]:.0f}, crossover={crossover}, result={result}")
        return result
    except Exception as e:
        print(f"MA 전환 분석 실패: {e}")
        return False

def check_institutional_buying(code, start_date, end_date, window=20):
    """기관 순매수 확인"""
    try:
        df_inst = stock.get_market_trading_value_by_investor(start_date.strftime('%Y%m%d'),
                                                            end_date.strftime('%Y%m%d'),
                                                            code,
                                                            "연기금")
        if not df_inst.empty and '매수' in df_inst.columns and '매도' in df_inst.columns:
            inst_trend = (df_inst['매수'] - df_inst['매도']).tail(window).mean()
            result = inst_trend > 0
            print(f"  기관 매수 체크: inst_trend={inst_trend:.0f}, result={result}")
            return result
        print("  기관 매수 체크: 연기금 데이터 없음")
        return False
    except Exception as e:
        print(f"기관 매수 분석 실패: {e}")
        return False

def process_stock(stock_info):
    """종목 데이터 분석"""
    try:
        code, market_cap = stock_info
        
        # 날짜 검증
        if start_date is None or end_date is None:
            logger.error(f"날짜 변수가 초기화되지 않았습니다: start_date={start_date}, end_date={end_date}")
            return None
            
        # 날짜 형식 문자열로 변환
        try:
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
        except Exception as e:
            logger.error(f"날짜 형식 변환 오류 ({code}): {str(e)}")
            return None
        
        # 거래량 급증 확인
        df = safe_stock_api_call(
            stock.get_market_ohlcv_by_date,
            start_date_str,
            end_date_str,
            code,
            adjusted=True
        )
        
        if df is None or df.empty:
            return None
        
        # 최소 100일 이상의 데이터가 필요
        if len(df) < 100:
            return None
        
        # 볼륨 분석
        df['volume_ma20'] = df['거래량'].rolling(window=20).mean()
        
        # 최근 5일 동안 한 번이라도 거래량이 20일 평균보다 100% 이상 높은 경우
        recent_volume_surge = df.iloc[-5:]['거래량'] > df.iloc[-5:]['volume_ma20'] * 2.0
        if not recent_volume_surge.any():
            return None
        
        # 가격 움직임 분석 - 최근 상승 확인
        df['ma20'] = df['종가'].rolling(window=20).mean()
        df['ma60'] = df['종가'].rolling(window=60).mean()
        df['ma120'] = df['종가'].rolling(window=120).mean()
        
        # 최근 골든 크로스 (20일선이 60일선 위로) 확인
        if df.iloc[-1]['ma20'] <= df.iloc[-1]['ma60']:
            return None
        
        # 60일선이 120일선 위로 올라오는지 확인
        if df.iloc[-1]['ma60'] <= df.iloc[-1]['ma120']:
            return None
        
        # 주가가 모든 이동평균선 위에 있는지 확인
        if df.iloc[-1]['종가'] <= df.iloc[-1]['ma20']:
            return None
        
        # 소형주이고 KOSDAQ 종목 위주로 확인
        if market_cap > 2_000_000_000_000:  # 2조원 이상은 제외
            return None
        
        # 최근 5일간 상승 추세 확인
        price_increase = df.iloc[-5:]['종가'].pct_change().dropna()
        if not (price_increase > 0).sum() >= 3:  # 최근 5일 중 3일 이상 상승
            return None
        
        return code
    except Exception as e:
        logger.error(f"종목 처리 중 오류 발생 ({code}): {str(e)}")
        return None

def format_stock_message(stock_codes, stock_names):
    """텔레그램 메시지 포맷팅"""
    if not stock_codes:
        return "오늘의 관심 종목이 없습니다."
    
    message = "🔍 *오늘의 급등 관심 종목* 🔍\n\n"
    
    for code, name in zip(stock_codes, stock_names):
        try:
            # 당일 시세 정보 가져오기
            ohlcv = safe_stock_api_call(
                stock.get_market_ohlcv_by_date,
                end_date.strftime('%Y%m%d'),
                end_date.strftime('%Y%m%d'),
                code
            )
            
            if ohlcv is None or ohlcv.empty:
                continue
                
            current_price = ohlcv.iloc[-1]['종가']
            change_rate = ohlcv.iloc[-1]['등락률']
            volume = ohlcv.iloc[-1]['거래량']
            
            # 외국인 매매 동향
            foreigner = safe_stock_api_call(
                stock.get_market_trading_value_by_date,
                end_date.strftime('%Y%m%d'),
                end_date.strftime('%Y%m%d'),
                code
            )
            
            foreigner_status = "정보 없음"
            if foreigner is not None and not foreigner.empty and '외국인순매수' in foreigner.columns:
                foreigner_buy = foreigner.iloc[-1]['외국인순매수']
                if foreigner_buy > 0:
                    foreigner_status = f"매수 {foreigner_buy:,.0f}원"
                else:
                    foreigner_status = f"매도 {abs(foreigner_buy):,.0f}원"
            
            # 기관 매매 동향
            institution = safe_stock_api_call(
                stock.get_market_trading_value_by_date,
                end_date.strftime('%Y%m%d'),
                end_date.strftime('%Y%m%d'),
                code
            )
            
            institution_status = "정보 없음"
            if institution is not None and not institution.empty and '기관순매수' in institution.columns:
                institution_buy = institution.iloc[-1]['기관순매수']
                if institution_buy > 0:
                    institution_status = f"매수 {institution_buy:,.0f}원"
                else:
                    institution_status = f"매도 {abs(institution_buy):,.0f}원"
            
            message += f"*{name}* ({code})\n"
            message += f"현재가: {current_price:,}원 ({change_rate:+.2f}%)\n"
            message += f"거래량: {volume:,}\n"
            message += f"외국인: {foreigner_status}\n"
            message += f"기관: {institution_status}\n\n"
            
        except Exception as e:
            logger.error(f"메시지 포맷팅 중 오류 ({code}): {str(e)}")
            message += f"*{name}* ({code}) - 상세 정보 로딩 중 오류 발생\n\n"
    
    message += "주의: 과거의 급등 패턴을 기반으로 분석한 종목으로, 투자 결정은 본인의 책임 하에 신중하게 진행하세요."
    
    return message

def save_to_database(stock_codes, stock_names):
    """분석 결과를 Supabase에 저장"""
    try:
        if not stock_codes:
            logger.info("저장할 데이터가 없습니다.")
            return True
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        stock_data = []
        for code, name in zip(stock_codes, stock_names):
            try:
                # 당일 시세 정보 가져오기
                ohlcv = safe_stock_api_call(
                    stock.get_market_ohlcv_by_date,
                    end_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d'),
                    code
                )
                
                if ohlcv is None or ohlcv.empty:
                    continue
                    
                current_price = ohlcv.iloc[-1]['종가']
                change_rate = ohlcv.iloc[-1]['등락률']
                
                stock_data.append({
                    'date': today,
                    'code': code,
                    'name': name,
                    'price': float(current_price),
                    'change_rate': float(change_rate)
                })
                
            except Exception as e:
                logger.error(f"데이터베이스 저장 준비 중 오류 ({code}): {str(e)}")
        
        # Supabase에 저장
        market_type = ""
        for code in stock_codes:
            if stock.get_market_ticker_name(code).endswith('KOSPI'):
                market_type = "kospi_stocks"
                break
            elif stock.get_market_ticker_name(code).endswith('KOSDAQ'):
                market_type = "kosdaq_stocks"
                break
        
        # 기본값 설정
        if not market_type:
            market_type = "kospi_stocks" 
            
        result = save_stock_data(stock_data, market_type)
        return result
        
    except Exception as e:
        logger.error(f"데이터베이스 저장 중 오류: {str(e)}")
        return False

def run_analysis():
    """급등주 분석 실행"""
    global start_date, end_date
    
    logger.info("급등주 분석 시작...")

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        logger.info("주식 데이터 수집 중...")
        
        # 날짜 형식 문자열로 변환
        try:
            end_date_str = end_date.strftime('%Y%m%d')
        except Exception as e:
            error_msg = f"날짜 형식 변환 오류: {str(e)}"
            logger.error(error_msg)
            send_telegram_message(f"급등주 분석 중 오류 발생: {error_msg}", is_kospi=True)
            return [], []
            
        all_stocks = safe_stock_api_call(stock.get_market_ticker_list, date=end_date_str)
        
        if not all_stocks:
            error_msg = "주식 목록을 가져오는 데 실패했습니다."
            logger.error(error_msg)
            send_telegram_message(f"급등주 분석 중 오류 발생: {error_msg}", is_kospi=True)
            return [], []

        # 우선주 및 스팩주 제외
        filtered_stocks = []
        for code in all_stocks:
            name = safe_stock_api_call(stock.get_market_ticker_name, code)
            if name and not code.endswith(('5', '7', '9')) and '우' not in name and '스팩' not in name and not code.startswith('43'):
                filtered_stocks.append(code)

        logger.info(f"종목 필터링 완료: {len(filtered_stocks)}개 종목 (우선주 및 스팩주 제외)")
        
        # 시가총액 가져오기
        marcap_dict = {}
        for code in filtered_stocks:
            try:
                marcap = safe_stock_api_call(
                    stock.get_market_cap_by_date, 
                    end_date.strftime('%Y%m%d'),
                    end_date.strftime('%Y%m%d'),
                    code
                )
                
                if marcap is not None and not marcap.empty:
                    marcap_dict[code] = marcap['시가총액'].iloc[-1]
            except Exception as e:
                logger.error(f"시가총액 가져오기 실패 ({code}): {str(e)}")

        small_caps = [(code, marcap_dict.get(code, 0)) for code in filtered_stocks]
        logger.info(f"분석 대상 종목: {len(small_caps)}개")

        # 종목명 사전 생성
        code_to_name = {}
        for code in filtered_stocks:
            name = safe_stock_api_call(stock.get_market_ticker_name, code)
            if name:
                code_to_name[code] = name

        logger.info("종목 분석 시작...")
        
        # 멀티프로세싱 풀 생성
        processes = min(cpu_count(), 4)  # CPU 코어 수와 4 중 작은 값 사용
        with Pool(processes) as p:
            results = list(tqdm(p.imap(process_stock, small_caps), total=len(small_caps)))

        # 결과 필터링
        selected_stocks = [code for code in results if code]
        selected_names = [code_to_name.get(code, "Unknown") for code in selected_stocks]

        logger.info(f"분석 완료: {len(selected_names)}개 종목 발견")

        # Supabase에 저장
        logger.info("데이터베이스 저장 중...")
        save_result = save_to_database(selected_stocks, selected_names)
        if save_result:
            logger.info("데이터베이스 저장 완료")
        else:
            logger.warning("데이터베이스 저장 실패")

        # 텔레그램 전송 (코스피/코스닥 봇으로 전송)
        message = format_stock_message(selected_stocks, selected_names)
        send_telegram_message(message, is_kospi=True)
        logger.info("텔레그램 메시지 전송 완료")

        return selected_stocks, selected_names

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"급등주 분석 중 오류 발생: {str(e)}\n{error_traceback}")
        send_telegram_message(f"급등주 분석 중 오류 발생: {str(e)}", is_kospi=True)
        return [], []

def should_run():
    """실행 시간 확인"""
    kr_tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(kr_tz)
    target_time = KOSPI_EXECUTION_TIME.split(':')
    return now.hour == int(target_time[0]) and now.minute == int(target_time[1])

def main():
    """메인 함수"""
    print("급등주 포착 프로그램 시작")
    print(f"실행 예정 시간: {KOSPI_EXECUTION_TIME}")

    # 개발 중에는 바로 실행 (주석 해제)
    # run_analysis()
    
    while True:
        kr_time = datetime.now(pytz.timezone('Asia/Seoul'))

        if should_run() and kr_time.weekday() < 5:  # 평일에만 실행
            print(f"\n실행 시작: {kr_time}")
            run_analysis()
            # 실행 후 1분 대기 (같은 시간에 중복 실행 방지)
            time.sleep(60)

        print(f"현재 시간: {kr_time.strftime('%Y-%m-%d %H:%M:%S')} 대기 중...")
        time.sleep(30)  # 30초마다 확인

if __name__ == "__main__":
    main() 
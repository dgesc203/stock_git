import pandas as pd
import numpy as np
from pykrx import stock
from datetime import datetime, timedelta
from collections import Counter
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import time
import pytz
import os
import logging
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
file_handler = logging.FileHandler('wave_analysis.log')
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

def process_stock(stock_info):
    """
    파동주 분석 - 피보나치 되돌림 수준을 통해 파동 패턴 분석
    
    Args:
        stock_info: (종목코드, 시가총액) 튜플
    
    Returns:
        분석 결과 딕셔너리 또는 None
    """
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
        
        # 최소 2년치 주봉 데이터 필요
        df = safe_stock_api_call(
            stock.get_market_ohlcv_by_date,
            start_date_str,
            end_date_str,
            code,
            adjusted=True,
            freq='w'  # 주봉 데이터
        )
        
        if df is None or df.empty or len(df) < 52:  # 최소 1년치(52주) 데이터 필요
            return None
            
        # 이동평균선 계산
        df['ma20'] = df['종가'].rolling(window=20).mean()
        df['ma60'] = df['종가'].rolling(window=60).mean()
        
        # 볼린저 밴드 계산 (20일 기준)
        df['bol_mid'] = df['종가'].rolling(window=20).mean()
        bol_std = df['종가'].rolling(window=20).std()
        df['bol_upper'] = df['bol_mid'] + 2 * bol_std
        df['bol_lower'] = df['bol_mid'] - 2 * bol_std
        
        # 최근 피크와 저점 찾기
        recent_df = df.iloc[-52:]  # 최근 1년
        
        # 고점 찾기 (전/후 봉보다 높은 봉)
        peaks = []
        for i in range(1, len(recent_df) - 1):
            if recent_df.iloc[i]['고가'] > recent_df.iloc[i-1]['고가'] and recent_df.iloc[i]['고가'] > recent_df.iloc[i+1]['고가']:
                peaks.append((i, recent_df.iloc[i]['고가']))
        
        # 저점 찾기 (전/후 봉보다 낮은 봉)
        troughs = []
        for i in range(1, len(recent_df) - 1):
            if recent_df.iloc[i]['저가'] < recent_df.iloc[i-1]['저가'] and recent_df.iloc[i]['저가'] < recent_df.iloc[i+1]['저가']:
                troughs.append((i, recent_df.iloc[i]['저가']))
        
        # 피크와 저점이 충분히 없으면 패턴 없음
        if len(peaks) < 2 or len(troughs) < 2:
            return None
            
        # 가장 최근 고점과 저점 찾기
        latest_peak = max(peaks, key=lambda x: x[0])
        latest_trough = max(troughs, key=lambda x: x[0])
        
        # 현재가
        current_price = df.iloc[-1]['종가']
        
        # 파동 패턴 검사를 위한 고점과 저점 정렬
        if latest_peak[0] > latest_trough[0]:
            # 고점 → 저점 → 현재 (하락 후 반등 가능성)
            wave_high = latest_peak[1]
            wave_low = latest_trough[1]
            pattern = "하락 후 반등"
        else:
            # 저점 → 고점 → 현재 (상승 후 조정 가능성)
            prev_peak_idx = [p[0] for p in peaks if p[0] < latest_trough[0]]
            if not prev_peak_idx:
                return None
                
            prev_peak = peaks[peaks.index(max(peaks, key=lambda x: x[0] if x[0] < latest_trough[0] else -1))]
            wave_high = latest_peak[1]
            wave_low = latest_trough[1]
            pattern = "상승 후 조정"
        
        # 피보나치 되돌림 수준 계산
        fib_levels = {}
        if pattern == "하락 후 반등":
            fib_range = wave_high - wave_low
            fib_levels = {
                "0.0": wave_low,
                "0.236": wave_low + 0.236 * fib_range,
                "0.382": wave_low + 0.382 * fib_range,
                "0.5": wave_low + 0.5 * fib_range,
                "0.618": wave_low + 0.618 * fib_range,
                "0.786": wave_low + 0.786 * fib_range,
                "1.0": wave_high
            }
        else:
            fib_range = wave_high - wave_low
            fib_levels = {
                "0.0": wave_high,
                "0.236": wave_high - 0.236 * fib_range,
                "0.382": wave_high - 0.382 * fib_range,
                "0.5": wave_high - 0.5 * fib_range,
                "0.618": wave_high - 0.618 * fib_range,
                "0.786": wave_high - 0.786 * fib_range,
                "1.0": wave_low
            }
        
        # 현재가가 어느 피보나치 수준에 있는지 확인
        current_fib = None
        for level in ["0.0", "0.236", "0.382", "0.5", "0.618", "0.786", "1.0"]:
            if level == "0.0":
                if current_price <= fib_levels["0.0"] and pattern == "하락 후 반등":
                    current_fib = "0.0"
                    break
                elif current_price >= fib_levels["0.0"] and pattern == "상승 후 조정":
                    current_fib = "0.0"
                    break
            elif level == "1.0":
                if current_price >= fib_levels["1.0"] and pattern == "하락 후 반등":
                    current_fib = "1.0"
                    break
                elif current_price <= fib_levels["1.0"] and pattern == "상승 후 조정":
                    current_fib = "1.0"
                    break
            else:
                next_level = ["0.236", "0.382", "0.5", "0.618", "0.786", "1.0"][["0.0", "0.236", "0.382", "0.5", "0.618", "0.786"].index(level)]
                if pattern == "하락 후 반등":
                    if fib_levels[level] <= current_price < fib_levels[next_level]:
                        current_fib = level
                        break
                else:
                    if fib_levels[next_level] < current_price <= fib_levels[level]:
                        current_fib = level
                        break
        
        # 유망 파동주 조건
        # 1. 현재 주가가 특정 피보나치 레벨에 있고
        # 2. RSI 지표가 과매수/과매도 상태가 아니며
        # 3. 최근 트렌드가 반전 신호를 보이는 경우
        
        # RSI 계산 (14일 기준)
        delta = df['종가'].diff()
        gain = delta.mask(delta < 0, 0)
        loss = -delta.mask(delta > 0, 0)
        avg_gain = gain.rolling(window=14).mean()
        avg_loss = loss.rolling(window=14).mean()
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))
        
        current_rsi = df.iloc[-1]['rsi']
        
        # MACD 계산
        exp12 = df['종가'].ewm(span=12, adjust=False).mean()
        exp26 = df['종가'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp12 - exp26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # 최근 MACD 방향
        macd_direction = "상승" if df.iloc[-1]['macd'] > df.iloc[-2]['macd'] else "하락"
        macd_cross = (df.iloc[-2]['macd'] < df.iloc[-2]['macd_signal'] and df.iloc[-1]['macd'] > df.iloc[-1]['macd_signal']) or \
                    (df.iloc[-2]['macd'] > df.iloc[-2]['macd_signal'] and df.iloc[-1]['macd'] < df.iloc[-1]['macd_signal'])
        
        # 최근 5봉 전체 움직임
        recent_trend = "상승" if df.iloc[-1]['종가'] > df.iloc[-5]['종가'] else "하락"
        
        # 볼린저 밴드 상태
        in_lower_band = df.iloc[-1]['종가'] < df.iloc[-1]['bol_lower']
        in_upper_band = df.iloc[-1]['종가'] > df.iloc[-1]['bol_upper']
        
        # 유망 파동주 판별
        is_promising = False
        if pattern == "하락 후 반등":
            # 하락 후 0.382~0.5 되돌림 구간에서 RSI가 30~50 사이이고 MACD가 상승 전환
            if current_fib in ["0.382", "0.5"] and 30 <= current_rsi <= 50 and macd_direction == "상승":
                is_promising = True
            # 또는 0.618 수준에서 RSI가 50~60이고 볼린저 밴드 중앙 근처
            elif current_fib == "0.618" and 50 <= current_rsi <= 60 and not (in_lower_band or in_upper_band):
                is_promising = True
        else:
            # 상승 후 0.382~0.5 조정 구간에서 RSI가 40~60 사이이고 MACD가 하락 둔화
            if current_fib in ["0.382", "0.5"] and 40 <= current_rsi <= 60 and macd_direction == "상승":
                is_promising = True
            # 또는 0.618 수준까지 조정 후 RSI가 30~40이고 볼린저 밴드 하단 근처
            elif current_fib == "0.618" and 30 <= current_rsi <= 40 and in_lower_band:
                is_promising = True
        
        if not is_promising:
            return None
            
        # 티커명 가져오기
        name = safe_stock_api_call(stock.get_market_ticker_name, code)
        
        # 유망 종목 리턴
        return {
            'code': code,
            'name': name,
            'pattern': pattern,
            'fib_level': current_fib,
            'rsi': current_rsi,
            'macd_direction': macd_direction,
            'macd_cross': macd_cross,
            'price': current_price,
            'wave_high': wave_high,
            'wave_low': wave_low
        }
        
    except Exception as e:
        logger.error(f"파동주 분석 중 오류 발생 ({code}): {str(e)}")
        return None

def format_wave_message(results):
    """파동주 분석 결과 메시지 포맷팅"""
    if not results:
        return "오늘 감지된 파동주가 없습니다."
    
    message = "🌊 *파동주 분석 결과* 🌊\n\n"
    
    # 패턴별 분류
    rebound_stocks = [r for r in results if r['pattern'] == "하락 후 반등"]
    correction_stocks = [r for r in results if r['pattern'] == "상승 후 조정"]
    
    # 반등 예상 종목
    if rebound_stocks:
        message += "📈 *반등 예상 종목*\n"
        for stock in rebound_stocks:
            message += f"*{stock['name']}* ({stock['code']})\n"
            message += f"현재가: {stock['price']:,}원\n"
            message += f"피보나치: {stock['fib_level']} 수준\n"
            message += f"RSI: {stock['rsi']:.1f}\n"
            message += f"고점: {stock['wave_high']:,}원 / 저점: {stock['wave_low']:,}원\n"
            message += f"MACD: {stock['macd_direction']} {'(골든크로스)' if stock['macd_cross'] and stock['macd_direction'] == '상승' else ''}\n\n"
    
    # 조정 후 매수 관심 종목
    if correction_stocks:
        message += "🔍 *조정 후 매수 관심 종목*\n"
        for stock in correction_stocks:
            message += f"*{stock['name']}* ({stock['code']})\n"
            message += f"현재가: {stock['price']:,}원\n"
            message += f"피보나치: {stock['fib_level']} 수준\n"
            message += f"RSI: {stock['rsi']:.1f}\n"
            message += f"고점: {stock['wave_high']:,}원 / 저점: {stock['wave_low']:,}원\n"
            message += f"MACD: {stock['macd_direction']} {'(데드크로스)' if stock['macd_cross'] and stock['macd_direction'] == '하락' else ''}\n\n"
    
    message += "⚠️ 주의: 피보나치 되돌림을 이용한 파동 분석은 참고용으로만 활용하시고, 실제 투자는 추가적인 분석과 함께 신중하게 결정하세요."
    
    return message

def save_to_database(results):
    """파동주 분석 결과를 Supabase에 저장"""
    try:
        if not results:
            logger.info("저장할 파동주 데이터가 없습니다.")
            return True
            
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 데이터 구성
        stock_data = []
        for result in results:
            try:
                stock_data.append({
                    'date': today,
                    'code': result['code'],
                    'name': result['name'],
                    'price': float(result['price']),
                    'change_rate': 0  # 변화율은 기본값으로 0 설정
                })
            except Exception as e:
                logger.error(f"파동주 데이터 저장 준비 중 오류 ({result['code']}): {str(e)}")
        
        # 시장 타입 확인 (더 정확한 방법 필요)
        market_type = "kospi_stocks"  # 기본값
        
        # Supabase에 저장
        result = save_stock_data(stock_data, market_type)
        return result
        
    except Exception as e:
        logger.error(f"파동주 데이터베이스 저장 중 오류: {str(e)}")
        return False

def run_analysis():
    """파동주 분석 실행"""
    global start_date, end_date
    
    logger.info("파동주 분석 시작...")

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*3)  # 3년치 데이터

        logger.info("주식 데이터 수집 중...")
        
        # 날짜 형식 문자열로 변환
        try:
            end_date_str = end_date.strftime('%Y%m%d')
        except Exception as e:
            error_msg = f"날짜 형식 변환 오류: {str(e)}"
            logger.error(error_msg)
            send_telegram_message(f"파동주 분석 중 오류 발생: {error_msg}", is_kospi=True)
            return []
            
        all_stocks = safe_stock_api_call(stock.get_market_ticker_list, date=end_date_str)
        
        if not all_stocks:
            error_msg = "주식 목록을 가져오는 데 실패했습니다."
            logger.error(error_msg)
            send_telegram_message(f"파동주 분석 중 오류 발생: {error_msg}", is_kospi=True)
            return []

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

        stocks_to_analyze = [(code, marcap_dict.get(code, 0)) for code in filtered_stocks]
        logger.info(f"분석 대상 종목: {len(stocks_to_analyze)}개")

        logger.info("피보나치 되돌림 분석 시작 (주봉 기준)...")
        
        # 멀티프로세싱 풀 생성
        processes = min(cpu_count(), 4)  # CPU 코어 수와 4 중 작은 값 사용
        with Pool(processes) as p:
            results = list(tqdm(p.imap(process_stock, stocks_to_analyze), total=len(stocks_to_analyze)))

        # 결과 필터링
        selected_results = [result for result in results if result]
        logger.info(f"분석 완료: {len(selected_results)}개 종목 발견")

        # Supabase에 저장
        logger.info("데이터베이스 저장 중...")
        save_result = save_to_database(selected_results)
        if save_result:
            logger.info("데이터베이스 저장 완료")
        else:
            logger.warning("데이터베이스 저장 실패")

        # 텔레그램 전송 (코스피/코스닥 봇으로 전송)
        message = format_wave_message(selected_results)
        send_telegram_message(message, is_kospi=True)
        logger.info("텔레그램 메시지 전송 완료")

        return selected_results

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"파동주 분석 중 오류 발생: {str(e)}\n{error_traceback}")
        send_telegram_message(f"파동주 분석 중 오류 발생: {str(e)}", is_kospi=True)
        return []

if __name__ == "__main__":
    run_analysis() 
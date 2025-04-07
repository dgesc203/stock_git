import yfinance as yf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import logging
import traceback
import time

from utils.telegram_service import send_telegram_message, send_telegram_image
from config.config import TQQQ_EXECUTION_TIME

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

# 파일 핸들러
file_handler = logging.FileHandler('tqqq_analysis.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# 콘솔 핸들러
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def safe_data_fetch(ticker, period="1y", interval="1d", retries=3, delay=2):
    """
    yfinance에서 안전하게 데이터를 가져오는 함수
    
    Args:
        ticker (str): 주식 티커 심볼
        period (str): 데이터 기간
        interval (str): 데이터 간격
        retries (int): 재시도 횟수
        delay (int): 재시도 간 대기 시간(초)
        
    Returns:
        DataFrame: 주식 데이터 또는 None
    """
    for attempt in range(retries):
        try:
            stock_data = yf.Ticker(ticker)
            df = stock_data.history(period=period, interval=interval)
            if df.empty:
                logger.warning(f"{ticker} 데이터가 비어 있습니다. 재시도 중... ({attempt+1}/{retries})")
                continue
            return df
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"{ticker} 데이터 가져오기 실패: {str(e)}. 재시도 중... ({attempt+1}/{retries})")
                time.sleep(delay)
            else:
                logger.error(f"{ticker} 데이터 가져오기 최종 실패: {str(e)}")
                return None

def get_tqqq_data():
    """
    TQQQ 데이터 가져오기 (최근 250일)
    
    Returns:
        pandas DataFrame: TQQQ 주가 데이터
    """
    try:
        tqqq = yf.Ticker("TQQQ")
        hist = tqqq.history(period="1y")  # 최근 1년 데이터
        
        if hist.empty:
            logger.error("TQQQ 데이터를 가져오는데 실패했습니다.")
            return None
            
        return hist
    except Exception as e:
        logger.error(f"TQQQ 데이터 조회 오류: {e}")
        return None

def analyze_tqqq():
    """
    TQQQ 200일 이동평균선과 엔벨로프 분석
    
    Returns:
        dict: 분석 결과 데이터
        - close_price: 종가
        - ma200: 200일 이동평균
        - envelope: 엔벨로프선 (MA200 + 10%)
        - recommendation: 추천 (SGOV, TQQQ, SPLG)
    """
    tqqq_data = get_tqqq_data()
    
    if tqqq_data is None:
        return None
    
    # 200일 이동평균 계산
    tqqq_data['MA200'] = tqqq_data['Close'].rolling(window=200).mean()
    
    # 10% 엔벨로프 계산 (MA200 + 10%)
    tqqq_data['Envelope'] = tqqq_data['MA200'] * 1.10
    
    # 최근 데이터
    latest = tqqq_data.iloc[-1]
    close_price = latest['Close']
    ma200 = latest['MA200']
    envelope = latest['Envelope']
    
    # 추천 계산
    if close_price < ma200:
        recommendation = "SGOV"  # 200일선 아래
    elif close_price >= ma200 and close_price < envelope:
        recommendation = "TQQQ"  # 200일선 위, 엔벨로프 아래
    else:
        recommendation = "SPLG"  # 엔벨로프 위
    
    # 결과 차트 생성
    chart_path = generate_tqqq_chart(tqqq_data)
    
    return {
        "close_price": close_price,
        "ma200": ma200,
        "envelope": envelope,
        "diff": close_price - ma200,
        "recommendation": recommendation,
        "chart_path": chart_path
    }

def generate_tqqq_chart(tqqq_data):
    """
    TQQQ 차트 생성 및 저장
    
    Args:
        tqqq_data: TQQQ 주가 데이터 (DataFrame)
    """
    try:
        plt.figure(figsize=(12, 6))
        
        # 종가, 200일 이동평균, 엔벨로프 그래프
        plt.plot(tqqq_data.index[-100:], tqqq_data['Close'][-100:], label='TQQQ 종가', color='blue')
        plt.plot(tqqq_data.index[-100:], tqqq_data['MA200'][-100:], label='200일 이동평균', color='red')
        plt.plot(tqqq_data.index[-100:], tqqq_data['Envelope'][-100:], label='엔벨로프 (MA200 + 10%)', color='green', linestyle='--')
        
        # 현재 날짜 추가
        current_date = datetime.now().strftime('%Y-%m-%d')
        plt.title(f'TQQQ 200일 이동평균 및 10% 엔벨로프 ({current_date})')
        plt.xlabel('날짜')
        plt.ylabel('가격 ($)')
        plt.legend()
        plt.grid(True)
        
        # 수치 텍스트로 표시
        latest = tqqq_data.iloc[-1]
        close_price = latest['Close']
        ma200 = latest['MA200']
        envelope = latest['Envelope']
        
        plt.figtext(0.02, 0.95, f'종가: ${close_price:.2f}', fontsize=9)
        plt.figtext(0.02, 0.92, f'200일선: ${ma200:.2f}', fontsize=9)
        plt.figtext(0.02, 0.89, f'엔벨로프: ${envelope:.2f}', fontsize=9)
        
        # 차트 저장
        chart_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, f'tqqq_chart_{current_date}.png')
        plt.savefig(chart_path)
        plt.close()
        
        logger.info(f"TQQQ 차트 생성 완료: {chart_path}")
        return chart_path
    except Exception as e:
        logger.error(f"차트 생성 오류: {e}")
        return None

def format_tqqq_message(result):
    """
    TQQQ 분석 결과 메시지 포맷팅
    
    Args:
        result: 분석 결과 딕셔너리
    
    Returns:
        formatted_message: 포맷팅된 메시지
    """
    if result is None:
        return "TQQQ 데이터를 가져오는데 실패했습니다."
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    message = f"📊 *TQQQ 200일선 분석* 📊\n({current_date})\n\n"
    message += f"TQQQ 종가: ${result['close_price']:.2f}\n"
    message += f"200일선: ${result['ma200']:.2f}\n"
    message += f"10%엔벨로프선: ${result['envelope']:.2f}\n"
    message += f"차이: ${result['diff']:.2f} ({(result['diff']/result['ma200']*100):.2f}%)\n\n"
    
    if result['recommendation'] == "SGOV":
        message += "⚠️ *현재 상태*: 200일선 아래\n"
        message += "💡 *추천*: SGOV (단기 국채 ETF) 구매"
    elif result['recommendation'] == "TQQQ":
        message += "✅ *현재 상태*: 200일선 위, 엔벨로프 아래\n"
        message += "💡 *추천*: TQQQ (3배 나스닥 ETF) 구매"
    else:
        message += "🔥 *현재 상태*: 엔벨로프 위\n"
        message += "💡 *추천*: SPLG (S&P 500 ETF) 구매"
    
    return message

def send_tqqq_alert():
    """TQQQ 알림 전송"""
    try:
        logger.info("TQQQ 분석 시작...")
    
        # TQQQ 분석
        result = analyze_tqqq()
        
        if result is None:
            message = "TQQQ 데이터를 가져오는데 실패했습니다."
            send_telegram_message(message)
            return
        
        # 메시지 포맷팅
        message = format_tqqq_message(result)
        
        # 텔레그램으로 차트 이미지 전송
        chart_path = result.get('chart_path')
        
        if chart_path and os.path.exists(chart_path):
            send_telegram_image(chart_path, caption=message)
            logger.info(f"TQQQ 차트 및 메시지 전송 완료: {chart_path}")
        else:
            send_telegram_message(message)
            logger.info("TQQQ 메시지만 전송 완료 (차트 없음)")
        
        logger.info("TQQQ 알림 전송 완료")
    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(f"TQQQ 알림 오류: {str(e)}\n{error_traceback}")
        send_telegram_message(f"TQQQ 알림 오류: {str(e)}")

def should_run():
    """실행 시간 확인"""
    now = datetime.now()
    target_time = TQQQ_EXECUTION_TIME.split(':')
    return now.hour == int(target_time[0]) and now.minute == int(target_time[1])

def main():
    """메인 함수"""
    print(f"TQQQ 분석 시작 (실행 예정 시간: {TQQQ_EXECUTION_TIME})")
    
    # 개발 중에는 바로 실행 (주석 해제)
    # send_tqqq_alert()
    
    while True:
        if should_run() and datetime.now().weekday() < 5:  # 평일에만 실행
            send_tqqq_alert()
            # 실행 후 1분 대기 (같은 시간에 중복 실행 방지)
            time.sleep(60)
        
        time.sleep(30)  # 30초마다 확인

if __name__ == "__main__":
    main() 
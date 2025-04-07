import yfinance as yf
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
from utils.telegram_service import send_telegram_message, send_telegram_image
from config.config import TQQQ_EXECUTION_TIME

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
            print("TQQQ 데이터를 가져오는데 실패했습니다.")
            return None
        
        return hist
    except Exception as e:
        print(f"TQQQ 데이터 조회 오류: {e}")
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
    generate_tqqq_chart(tqqq_data)
    
    return {
        "close_price": close_price,
        "ma200": ma200,
        "envelope": envelope,
        "diff": close_price - ma200,
        "recommendation": recommendation
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
        
        plt.title('TQQQ 200일 이동평균 및 10% 엔벨로프')
        plt.xlabel('날짜')
        plt.ylabel('가격 ($)')
        plt.legend()
        plt.grid(True)
        
        # 차트 저장
        chart_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
        os.makedirs(chart_dir, exist_ok=True)
        chart_path = os.path.join(chart_dir, 'tqqq_chart.png')
        plt.savefig(chart_path)
        plt.close()
        
        return chart_path
    except Exception as e:
        print(f"차트 생성 오류: {e}")
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
    
    message = "TQQQ 200일선 차트\n"
    message += f"TQQQ 종가: {result['close_price']:.2f}\n"
    message += f"200일선: {result['ma200']:.2f}\n"
    message += f"10%엔벨로프선: {result['envelope']:.2f}\n"
    message += f"차이: {result['diff']:.2f}\n"
    message += f"결과 - {result['recommendation']} 구매 추천"
    
    return message

def send_tqqq_alert():
    """TQQQ 알림 전송"""
    try:
        print("TQQQ 분석 시작...")
        
        # TQQQ 분석
        result = analyze_tqqq()
        
        if result is None:
            message = "TQQQ 데이터를 가져오는데 실패했습니다."
            send_telegram_message(message)
            return
        
        # 메시지 포맷팅
        message = format_tqqq_message(result)
        
        # 텔레그램으로 차트 이미지 전송
        chart_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            'data', 
            'tqqq_chart.png'
        )
        
        if os.path.exists(chart_path):
            send_telegram_image(chart_path, caption=message)
        else:
            send_telegram_message(message)
        
        print("TQQQ 알림 전송 완료")
    except Exception as e:
        print(f"TQQQ 알림 오류: {e}")

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
    import time
    main() 
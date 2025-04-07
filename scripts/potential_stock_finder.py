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

from utils.telegram_service import send_telegram_message
from utils.database import save_stock_data
from config.config import KOSPI_EXECUTION_TIME

# 글로벌 변수
start_date = None
end_date = None

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

def process_stock(args):
    """단일 종목 분석 처리"""
    code, marcap = args
    try:
        start_date_short = end_date - timedelta(days=365)
        df = stock.get_market_ohlcv_by_date(start_date_short.strftime('%Y%m%d'),
                                           end_date.strftime('%Y%m%d'),
                                           code)
        if len(df) < 60:
            print(f"종목 {code} | 데이터 부족 (len={len(df)}), 최소 60일 필요")
            return None

        inst_start_date = end_date - timedelta(days=20)
        print(f"종목 {code} 데이터 가져오기 성공 (len={len(df)}), 기간: {df.index[0]} ~ {df.index[-1]}")

        pass_volume = check_volume_spike(df)
        pass_marcap = check_market_cap(marcap)
        pass_ma240 = check_close_to_ma240(df)
        pass_transition = check_ma_transition(df)
        pass_inst = check_institutional_buying(code, inst_start_date, end_date)

        print(f"종목 {code} | Volume: {pass_volume} | Marcap: {pass_marcap} | MA240: {pass_ma240} | Transition: {pass_transition} | Inst: {pass_inst}")

        if pass_volume and pass_marcap and pass_ma240 and pass_transition and pass_inst:
            return code
        return None
    except Exception as e:
        print(f"종목 {code} 분석 실패: {e}")
        return None

def save_to_database(stocks, names):
    """분석 결과 Supabase에 저장"""
    try:
        today = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
        
        # KOSPI 종목 데이터 구성
        kospi_data = []
        kosdaq_data = []
        
        for idx, code in enumerate(stocks):
            name = names[idx]
            
            # 현재가 정보 가져오기
            ohlcv = stock.get_market_ohlcv_by_date(
                (datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
                datetime.now().strftime('%Y%m%d'),
                code
            )
            
            if ohlcv.empty:
                continue
                
            price = ohlcv['종가'].iloc[-1]
            
            # 전일대비 등락률
            prev_price = ohlcv['종가'].iloc[-2] if len(ohlcv) > 1 else price
            change_rate = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0
            
            # 시장 구분 (KOSPI/KOSDAQ)
            market_type = stock.get_market_ticker_market(code)
            
            stock_data = {
                'date': today,
                'code': code,
                'name': name,
                'price': float(price),
                'change_rate': float(change_rate)
            }
            
            if market_type == 'KOSPI':
                kospi_data.append(stock_data)
            else:
                kosdaq_data.append(stock_data)
        
        # Supabase에 저장
        if kospi_data:
            save_stock_data(kospi_data, 'KOSPI')
        
        if kosdaq_data:
            save_stock_data(kosdaq_data, 'KOSDAQ')
            
        return True
    except Exception as e:
        print(f"데이터베이스 저장 오류: {e}")
        return False

def format_stock_message(stocks, names):
    """텔레그램 메시지 포맷팅"""
    if not stocks:
        return "오늘 조건에 맞는 급등주가 없습니다."
    
    today = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
    message = f"📊 {today} 급등주 포착 결과\n\n"
    
    # 카테고리별로 구분
    kospi_stocks = []
    kosdaq_stocks = []
    
    for idx, code in enumerate(stocks):
        name = names[idx]
        market_type = stock.get_market_ticker_market(code)
        
        # 현재가 정보 가져오기
        try:
            ohlcv = stock.get_market_ohlcv_by_date(
                (datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
                datetime.now().strftime('%Y%m%d'),
                code
            )
            
            if not ohlcv.empty:
                price = ohlcv['종가'].iloc[-1]
                prev_price = ohlcv['종가'].iloc[-2] if len(ohlcv) > 1 else price
                change_rate = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0
                
                stock_info = f"{name} ({code}): {price:,.0f}원 ({change_rate:.2f}%)"
                
                if market_type == 'KOSPI':
                    kospi_stocks.append(stock_info)
                else:
                    kosdaq_stocks.append(stock_info)
            else:
                stock_info = f"{name} ({code}): 가격 정보 없음"
                
                if market_type == 'KOSPI':
                    kospi_stocks.append(stock_info)
                else:
                    kosdaq_stocks.append(stock_info)
        except Exception as e:
            stock_info = f"{name} ({code}): 조회 오류"
            
            if market_type == 'KOSPI':
                kospi_stocks.append(stock_info)
            else:
                kosdaq_stocks.append(stock_info)
    
    # 메시지 구성
    if kospi_stocks:
        message += "🔵 KOSPI 급등주:\n"
        for stock in kospi_stocks:
            message += f"• {stock}\n"
        message += "\n"
    
    if kosdaq_stocks:
        message += "🔴 KOSDAQ 급등주:\n"
        for stock in kosdaq_stocks:
            message += f"• {stock}\n"
    
    message += "\n조건: 거래량 급증, 시총 5000억↑, MA240 근처, MA20>MA60, 기관 순매수"
    
    return message

def run_analysis():
    """급등주 분석 실행"""
    global start_date, end_date

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)

        print("주식 데이터 수집 중...")
        all_stocks = stock.get_market_ticker_list(date=end_date.strftime('%Y%m%d'))

        # 우선주 및 스팩주 제외
        filtered_stocks = []
        for code in all_stocks:
            name = stock.get_market_ticker_name(code)
            if not code.endswith(('5', '7', '9')) and '우' not in name and '스팩' not in name and not code.startswith('43'):
                filtered_stocks.append(code)

        marcap_dict = {}
        for code in filtered_stocks:
            marcap = stock.get_market_cap_by_date(end_date.strftime('%Y%m%d'),
                                                 end_date.strftime('%Y%m%d'),
                                                 code)
            if not marcap.empty:
                marcap_dict[code] = marcap['시가총액'].iloc[-1]

        small_caps = [(code, marcap_dict.get(code, 0)) for code in filtered_stocks]
        print(f"전체 종목 분석: {len(small_caps)}개 종목 (우선주 및 스팩주 제외)")

        code_to_name = {code: stock.get_market_ticker_name(code) for code in filtered_stocks}

        print("종목 분석 시작...")
        with Pool(cpu_count()) as p:
            results = list(tqdm(p.imap(process_stock, small_caps), total=len(small_caps)))

        selected_stocks = [code for code in results if code]
        selected_names = [code_to_name[code] for code in selected_stocks]

        print(f"분석 완료: {len(selected_names)}개 종목 발견")

        # Supabase에 저장
        print("데이터베이스 저장 중...")
        save_result = save_to_database(selected_stocks, selected_names)
        if save_result:
            print("데이터베이스 저장 완료")
        else:
            print("데이터베이스 저장 실패")

        # 텔레그램 전송 (코스피/코스닥 봇으로 전송)
        message = format_stock_message(selected_stocks, selected_names)
        send_telegram_message(message, is_kospi=True)
        print("텔레그램 메시지 전송 완료")

        return selected_stocks, selected_names

    except Exception as e:
        print(f"에러 발생: {str(e)}")
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
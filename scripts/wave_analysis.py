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

from utils.telegram_service import send_telegram_message
from utils.database import save_stock_data
from config.config import KOSPI_EXECUTION_TIME

# 글로벌 변수
start_date = None
end_date = None

def get_weekly_data(code, start_date, end_date):
    """일별 데이터를 가져와서 주봉 데이터로 변환"""
    try:
        # 시작일을 충분히 과거로 설정 (주봉 데이터를 만들기 위해)
        extended_start = start_date - timedelta(days=365*2)
        daily_data = stock.get_market_ohlcv_by_date(
            extended_start.strftime('%Y%m%d'),
            end_date.strftime('%Y%m%d'),
            code
        )

        # 비어있는 경우 빈 데이터프레임 반환
        if daily_data.empty:
            return pd.DataFrame()

        # 주봉 데이터 생성
        weekly_data = daily_data.resample('W').agg({
            '시가': 'first',
            '고가': 'max',
            '저가': 'min',
            '종가': 'last',
            '거래량': 'sum'
        })

        # 결측치 제거
        weekly_data = weekly_data.dropna()

        return weekly_data
    except Exception as e:
        print(f"주봉 데이터 가져오기 실패 ({code}): {e}")
        return pd.DataFrame()

def calculate_bollinger_bands(df, window=20):
    """볼린저 밴드 계산"""
    try:
        # 볼린저 밴드 계산
        df['bollinger_mid'] = df['종가'].rolling(window=window).mean()
        std = df['종가'].rolling(window=window).std()
        df['bollinger_upper'] = df['bollinger_mid'] + 2 * std
        df['bollinger_lower'] = df['bollinger_mid'] - 2 * std

        # 볼린저 %B 값 계산
        df['bollinger_pctb'] = (df['종가'] - df['bollinger_lower']) / (df['bollinger_upper'] - df['bollinger_lower'])

        current_pctb = df['bollinger_pctb'].iloc[-1]
        print(f"  볼린저 밴드: %B={current_pctb:.2f} (0.0=하단, 0.5=중앙, 1.0=상단)")

        return df, current_pctb < 0.2
    except Exception as e:
        print(f"볼린저 밴드 계산 실패: {e}")
        return df, False

def calculate_obv(df):
    """OBV(On-Balance Volume) 계산"""
    try:
        # OBV 초기값 설정
        df['obv'] = 0
        df['obv'].iloc[0] = df['거래량'].iloc[0]
        
        # OBV 계산
        for i in range(1, len(df)):
            if df['종가'].iloc[i] > df['종가'].iloc[i-1]:
                df['obv'].iloc[i] = df['obv'].iloc[i-1] + df['거래량'].iloc[i]
            elif df['종가'].iloc[i] < df['종가'].iloc[i-1]:
                df['obv'].iloc[i] = df['obv'].iloc[i-1] - df['거래량'].iloc[i]
            else:
                df['obv'].iloc[i] = df['obv'].iloc[i-1]

        # OBV 상승/하락 확인 (10주 이동평균과 비교)
        df['obv_ma'] = df['obv'].rolling(10).mean()
        obv_rising = df['obv'].iloc[-1] > df['obv_ma'].iloc[-1]

        # 결과 로그
        print(f"  OBV 추세: {'상승' if obv_rising else '하락'}")

        return df, obv_rising
    except Exception as e:
        print(f"OBV 계산 실패: {e}")
        return df, False

def calculate_macd(df):
    """MACD(Moving Average Convergence/Divergence) 계산"""
    try:
        # MACD 계산
        exp1 = df['종가'].ewm(span=12, adjust=False).mean()
        exp2 = df['종가'].ewm(span=26, adjust=False).mean()
        df['macd'] = exp1 - exp2
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

        # MACD 히스토그램 확인
        hist_rising = (df['macd_hist'].iloc[-1] > 0 and
                      df['macd_hist'].iloc[-1] > df['macd_hist'].iloc[-2] > df['macd_hist'].iloc[-3])

        # 골든 크로스 확인
        golden_cross = (df['macd'].iloc[-2] < df['macd_signal'].iloc[-2] and
                        df['macd'].iloc[-1] > df['macd_signal'].iloc[-1])

        print(f"  MACD: 히스토그램 상승={hist_rising}, 골든크로스={golden_cross}")

        return df, hist_rising, golden_cross
    except Exception as e:
        print(f"MACD 계산 실패: {e}")
        return df, False, False

def calculate_rsi(df, period=14):
    """RSI(Relative Strength Index) 계산"""
    try:
        # 가격 변화 계산
        delta = df['종가'].diff()
        
        # 상승/하락 구분
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 평균 상승/하락 계산
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # RS 계산
        rs = avg_gain / avg_loss
        
        # RSI 계산
        df['rsi'] = 100 - (100 / (1 + rs))
        
        current_rsi = df['rsi'].iloc[-1]
        
        # RSI 과매도 확인 (30 이하)
        oversold = current_rsi < 30
        
        # RSI 반등 확인 (상승 추세)
        rsi_rising = df['rsi'].iloc[-1] > df['rsi'].iloc[-2] > df['rsi'].iloc[-3]
        
        print(f"  RSI 분석: RSI(14)={current_rsi:.2f}, 과매도={oversold}, 상승={rsi_rising}")
        return df, current_rsi, oversold, rsi_rising
    except Exception as e:
        print(f"RSI 계산 실패: {e}")
        return df, None, False, False

def check_volume_trend(df):
    """거래량 추세 분석"""
    try:
        # 거래량 이동평균
        df['vol_ma10'] = df['거래량'].rolling(10).mean()

        # 최근 거래량 증가 여부
        vol_increasing = df['거래량'].iloc[-1] > df['vol_ma10'].iloc[-1]

        # 최근 3주 거래량 감소 패턴 확인 (4파 특징)
        vol_decrease_pattern = (df['거래량'].iloc[-4] > df['거래량'].iloc[-3] > df['거래량'].iloc[-2])

        # 결과 로그
        print(f"  거래량 추세: 평균 대비 증가={vol_increasing}, 감소 패턴={vol_decrease_pattern}")

        return {
            'vol_increasing': vol_increasing,
            'vol_decrease_pattern': vol_decrease_pattern
        }
    except Exception as e:
        print(f"거래량 추세 분석 실패: {e}")
        return {'vol_increasing': False, 'vol_decrease_pattern': False}

def check_market_cap(marcap):
    """시가총액 확인 (3000억 이상)"""
    try:
        result = marcap >= 300000000000
        print(f"  시가총액: marcap={marcap:.0f}, result={result}")
        return result
    except Exception as e:
        print(f"시총 분석 실패: {e}")
        return False

def check_close_to_ma240(df):
    """240주 이동평균선 근처 확인"""
    try:
        if len(df) < 240:
            print(f"  MA240 체크: 데이터 부족으로 계산 불가 (len={len(df)})")
            return False

        df['ma240'] = df['종가'].rolling(240).mean()
        current_close = df['종가'].iloc[-1]
        ma240_value = df['ma240'].iloc[-1]

        ma_upper = ma240_value * 1.5
        ma_lower = ma240_value * 0.5

        result = (current_close >= ma_lower) and (current_close <= ma_upper)
        print(f"  MA240 범위 체크: close={current_close:.0f}, ma240={ma240_value:.0f}, upper={ma_upper:.0f}, lower={ma_lower:.0f}, result={result}")
        return result
    except Exception as e:
        print(f"MA240 분석 실패: {e}")
        return False

def identify_wave_pattern(df):
    """파동 패턴 및 피보나치 되돌림 분석"""
    try:
        # 최근 상승 고점과 저점 탐지 (간단한 구현)
        high_period = 20  # 고점 탐색 기간
        low_period = 10   # 저점 탐색 기간 (고점 이후)

        # 최근 N주 중 고점 찾기
        if len(df) < high_period + low_period:
            print(f"  파동 분석: 데이터 부족 (len={len(df)})")
            return None

        recent_highs = df['고가'].iloc[-high_period:]
        high_idx = recent_highs.idxmax()
        wave3_high = recent_highs.max()

        # 고점 이후 데이터에서 저점 찾기
        if high_idx in df.index:
            high_pos = df.index.get_loc(high_idx)
            if high_pos < len(df) - 1:
                post_high_data = df.iloc[high_pos:min(high_pos + low_period, len(df))]
                wave4_low = post_high_data['저가'].min()
                low_idx = post_high_data['저가'].idxmin()

                # 파동 간격 계산
                wave_range = wave3_high - wave4_low

                # 피보나치 되돌림 수준 계산 (3파 고점에서 하락)
                fib_382 = wave3_high - (wave_range * 0.382)  # 38.2% 되돌림
                fib_236 = wave3_high - (wave_range * 0.236)  # 23.6% 되돌림

                # 현재 가격이 피보나치 되돌림 구간에 있는지 확인
                current_price = df['종가'].iloc[-1]
                in_fib_zone = (current_price <= fib_236) and (current_price >= fib_382)

                # 최근 저점에서 반등했는지 확인
                recent_rebound = df['종가'].iloc[-1] > df['종가'].iloc[-2]

                print(f"  파동 분석: 3파 고점={wave3_high:.0f}, 4파 저점={wave4_low:.0f}")
                print(f"  피보나치: 0.236={fib_236:.0f}, 0.382={fib_382:.0f}, 현재가={current_price:.0f}")
                print(f"  피보나치 구간: {in_fib_zone}, 반등: {recent_rebound}")

                return {
                    'wave3_high': wave3_high,
                    'wave4_low': wave4_low,
                    'fib_236': fib_236,
                    'fib_382': fib_382,
                    'in_fib_zone': in_fib_zone,
                    'recent_rebound': recent_rebound
                }

        print("  파동 패턴 식별 불가")
        return None
    except Exception as e:
        print(f"파동 패턴 분석 실패: {e}")
        return None

def process_stock(args):
    """주식 파동 분석 처리"""
    code, marcap = args
    try:
        end_date_dt = end_date
        start_date_year = end_date_dt - timedelta(days=365*3)  # 3년 데이터

        # 주봉 데이터 가져오기
        weekly_df = get_weekly_data(code, start_date_year, end_date_dt)
        if weekly_df.empty or len(weekly_df) < 52:  # 최소 1년 데이터 필요
            print(f"종목 {code} | 주봉 데이터 부족 (len={len(weekly_df)}), 최소 52주 필요")
            return None

        print(f"종목 {code} 주봉 데이터 가져오기 성공 (len={len(weekly_df)}), 기간: {weekly_df.index[0]} ~ {weekly_df.index[-1]}")

        # 1. 시가총액 확인
        pass_marcap = check_market_cap(marcap)

        # 2. 지표 계산
        # 2.1 볼린저 밴드
        weekly_df, in_bollinger_low = calculate_bollinger_bands(weekly_df)

        # 2.2 OBV
        weekly_df, obv_rising = calculate_obv(weekly_df)

        # 2.3 MACD
        weekly_df, macd_hist_rising, macd_golden_cross = calculate_macd(weekly_df)

        # 2.4 RSI
        weekly_df, rsi, oversold, rsi_rising = calculate_rsi(weekly_df)

        # 3. 240주 이동평균선 확인
        pass_ma240 = check_close_to_ma240(weekly_df)

        # 4. 파동 패턴 및 피보나치 분석
        wave_data = identify_wave_pattern(weekly_df)
        pass_fib = wave_data is not None and wave_data['in_fib_zone'] and wave_data['recent_rebound']

        # 5. 거래량 추세 확인
        volume_data = check_volume_trend(weekly_df)

        # 종합 점수 계산 (각 지표별 가중치 적용)
        score = 0
        if pass_fib: score += 3  # 피보나치 일치는 높은 가중치
        if in_bollinger_low: score += 2  # 볼린저 밴드 하단은 중요 지표
        if oversold and rsi_rising: score += 3  # RSI 과매도에서 반등
        if macd_golden_cross: score += 2  # MACD 골든크로스
        if macd_hist_rising: score += 1  # MACD 히스토그램 상승
        if obv_rising: score += 1  # OBV 상승
        if volume_data['vol_decrease_pattern']: score += 1  # 거래량 감소 패턴
        if pass_ma240: score += 1  # MA240 범위 내
        if pass_marcap: score += 1  # 시가총액 충족

        print(f"종목 {code} | 종합점수: {score}/14 | 피보나치: {pass_fib} | 볼린저: {in_bollinger_low} | RSI: {oversold} | MACD교차: {macd_golden_cross}")

        # 최소 7점 이상 (50%)인 경우만 선택
        if score >= 7:
            return {
                'code': code,
                'score': score,
                'rsi': rsi,
                'fib_zone': wave_data['in_fib_zone'] if wave_data else False,
                'fib_data': wave_data,
                'bollinger_low': in_bollinger_low,
                'macd_cross': macd_golden_cross,
                'obv_rising': obv_rising,
                'rebound': wave_data['recent_rebound'] if wave_data else False,
                'vol_trend': volume_data['vol_decrease_pattern']
            }
        return None
    except Exception as e:
        print(f"종목 {code} 분석 실패: {e}")
        return None

def save_to_database(results):
    """분석 결과 Supabase에 저장"""
    try:
        today = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
        
        # KOSPI 종목 데이터 구성
        kospi_data = []
        kosdaq_data = []
        
        for result in results:
            if not result:
                continue
                
            code = result['code']
            
            # 현재가 정보 가져오기
            ohlcv = stock.get_market_ohlcv_by_date(
                (datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
                datetime.now().strftime('%Y%m%d'),
                code
            )
            
            if ohlcv.empty:
                continue
                
            name = stock.get_market_ticker_name(code)
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

def format_wave_message(results):
    """텔레그램 메시지 포맷팅"""
    if not results:
        return "오늘 조건에 맞는 파동주가 없습니다."
    
    today = datetime.now(pytz.timezone('Asia/Seoul')).strftime('%Y-%m-%d')
    message = f"📈 {today} 파동 분석 결과 (주봉 기준)\n\n"
    
    # 점수 순으로 정렬
    sorted_results = sorted(results, key=lambda x: x['score'] if x else 0, reverse=True)
    
    # 카테고리별로 구분
    kospi_stocks = []
    kosdaq_stocks = []
    
    for result in sorted_results:
        if not result:
            continue
            
        code = result['code']
        name = stock.get_market_ticker_name(code)
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
                
                indicators = []
                if result['fib_zone']: indicators.append("피보나치✓")
                if result['bollinger_low']: indicators.append("볼린저✓")
                if result['macd_cross']: indicators.append("MACD✓")
                
                stock_info = f"{name} ({code}): {price:,.0f}원 ({change_rate:.2f}%) [{result['score']}/14] - {', '.join(indicators)}"
                
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
        message += "🔵 KOSPI 파동주:\n"
        for stock in kospi_stocks:
            message += f"• {stock}\n"
        message += "\n"
    
    if kosdaq_stocks:
        message += "🔴 KOSDAQ 파동주:\n"
        for stock in kosdaq_stocks:
            message += f"• {stock}\n"
    
    message += "\n분석조건: 피보나치 되돌림, 볼린저 하단, RSI 과매도 반등, MACD 교차, 거래량 패턴"
    
    return message

def run_analysis():
    """파동주 분석 실행"""
    global start_date, end_date

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*3)  # 3년치 데이터

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

        stocks_to_analyze = [(code, marcap_dict.get(code, 0)) for code in filtered_stocks]
        print(f"전체 종목 분석: {len(stocks_to_analyze)}개 종목 (우선주 및 스팩주 제외)")

        print("피보나치 되돌림 분석 시작 (주봉 기준)...")
        with Pool(cpu_count()) as p:
            results = list(tqdm(p.imap(process_stock, stocks_to_analyze), total=len(stocks_to_analyze)))

        selected_results = [result for result in results if result]
        print(f"분석 완료: {len(selected_results)}개 종목 발견")

        # Supabase에 저장
        print("데이터베이스 저장 중...")
        save_result = save_to_database(selected_results)
        if save_result:
            print("데이터베이스 저장 완료")
        else:
            print("데이터베이스 저장 실패")

        # 텔레그램 전송 (코스피/코스닥 봇으로 전송)
        message = format_wave_message(selected_results)
        send_telegram_message(message, is_kospi=True)
        print("텔레그램 메시지 전송 완료")

        return selected_results

    except Exception as e:
        print(f"에러 발생: {str(e)}")
        send_telegram_message(f"파동주 분석 중 오류 발생: {str(e)}", is_kospi=True)
        return []

def should_run():
    """실행 시간 확인"""
    kr_tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(kr_tz)
    target_time = KOSPI_EXECUTION_TIME.split(':')
    return now.hour == int(target_time[0]) and now.minute == int(target_time[1])

def main():
    """메인 함수"""
    print("피보나치 주식 검색기 시작 (주봉 기준)")
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
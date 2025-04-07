import os
import time
import argparse
import schedule
import logging
from datetime import datetime
import pytz

from utils.database import create_tables
from scripts.tqqq_analysis import send_tqqq_alert
from scripts.potential_stock_finder import run_analysis as run_potential_stock_analysis
from scripts.wave_analysis import run_analysis as run_wave_analysis
from config.config import TQQQ_EXECUTION_TIME, KOSPI_EXECUTION_TIME

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("telebot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_schedules():
    """스케줄 설정"""
    # TQQQ 알림 (오전 9시)
    tqqq_hour, tqqq_minute = map(int, TQQQ_EXECUTION_TIME.split(':'))
    schedule.every().day.at(TQQQ_EXECUTION_TIME).do(send_tqqq_alert)
    logger.info(f"TQQQ 알림 스케줄 설정: 매일 {tqqq_hour:02d}:{tqqq_minute:02d}")
    
    # 주식 분석 (오후 5시)
    kospi_hour, kospi_minute = map(int, KOSPI_EXECUTION_TIME.split(':'))
    schedule.every().day.at(KOSPI_EXECUTION_TIME).do(run_stock_analysis)
    logger.info(f"주식 분석 스케줄 설정: 매일 {kospi_hour:02d}:{kospi_minute:02d}")

def run_stock_analysis():
    """급등주와 파동주 분석 실행"""
    kr_time = datetime.now(pytz.timezone('Asia/Seoul'))
    
    # 주말에는 실행하지 않음
    if kr_time.weekday() >= 5:  # 5: 토요일, 6: 일요일
        logger.info("주말이므로 주식 분석을 실행하지 않습니다.")
        return
    
    logger.info("주식 분석 시작...")
    
    # 급등주 포착
    try:
        logger.info("급등주 분석 시작...")
        run_potential_stock_analysis()
        logger.info("급등주 분석 완료")
    except Exception as e:
        logger.error(f"급등주 분석 중 오류 발생: {str(e)}")
    
    # 파동주 분석
    try:
        logger.info("파동주 분석 시작...")
        run_wave_analysis()
        logger.info("파동주 분석 완료")
    except Exception as e:
        logger.error(f"파동주 분석 중 오류 발생: {str(e)}")
    
    logger.info("주식 분석 완료")

def run_tqqq_alert():
    """TQQQ 알림 실행"""
    kr_time = datetime.now(pytz.timezone('Asia/Seoul'))
    
    # 주말에는 실행하지 않음
    if kr_time.weekday() >= 5:  # 5: 토요일, 6: 일요일
        logger.info("주말이므로 TQQQ 알림을 실행하지 않습니다.")
        return
    
    try:
        logger.info("TQQQ 알림 시작...")
        send_tqqq_alert()
        logger.info("TQQQ 알림 완료")
    except Exception as e:
        logger.error(f"TQQQ 알림 중 오류 발생: {str(e)}")

def run_scheduler():
    """스케줄러 실행"""
    setup_schedules()
    
    logger.info("스케줄러 시작...")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(30)  # 30초마다 체크
        except Exception as e:
            logger.error(f"스케줄러 실행 중 오류 발생: {str(e)}")
            time.sleep(60)  # 오류 발생 시 1분 대기

def run_all():
    """모든 분석 즉시 실행 (테스트용)"""
    logger.info("모든 분석 즉시 실행 시작...")
    
    # TQQQ 분석
    try:
        logger.info("TQQQ 알림 실행...")
        send_tqqq_alert()
        logger.info("TQQQ 알림 완료")
    except Exception as e:
        logger.error(f"TQQQ 알림 중 오류 발생: {str(e)}")
    
    # 급등주 분석
    try:
        logger.info("급등주 분석 시작...")
        run_potential_stock_analysis()
        logger.info("급등주 분석 완료")
    except Exception as e:
        logger.error(f"급등주 분석 중 오류 발생: {str(e)}")
    
    # 파동주 분석
    try:
        logger.info("파동주 분석 시작...")
        run_wave_analysis()
        logger.info("파동주 분석 완료")
    except Exception as e:
        logger.error(f"파동주 분석 중 오류 발생: {str(e)}")
    
    logger.info("모든 분석 실행 완료")

def run_specific_analysis(analysis_type):
    """특정 분석만 실행"""
    logger.info(f"{analysis_type} 분석 실행 시작...")
    
    if analysis_type == 'tqqq':
        try:
            send_tqqq_alert()
            logger.info("TQQQ 알림 완료")
        except Exception as e:
            logger.error(f"TQQQ 알림 중 오류 발생: {str(e)}")
    
    elif analysis_type == 'potential':
        try:
            run_potential_stock_analysis()
            logger.info("급등주 분석 완료")
        except Exception as e:
            logger.error(f"급등주 분석 중 오류 발생: {str(e)}")
    
    elif analysis_type == 'wave':
        try:
            run_wave_analysis()
            logger.info("파동주 분석 완료")
        except Exception as e:
            logger.error(f"파동주 분석 중 오류 발생: {str(e)}")
    
    else:
        logger.error(f"알 수 없는 분석 유형: {analysis_type}")
    
    logger.info(f"{analysis_type} 분석 실행 완료")

def main():
    parser = argparse.ArgumentParser(description='텔레그램 주식 알림 봇')
    parser.add_argument('--run', choices=['all', 'tqqq', 'potential', 'wave'], 
                        help='즉시 특정 분석을 실행 (all: 모든 분석, tqqq: TQQQ 알림, potential: 급등주 분석, wave: 파동주 분석)')
    parser.add_argument('--init-db', action='store_true', help='데이터베이스 초기화')
    
    args = parser.parse_args()
    
    # 데이터베이스 초기화
    if args.init_db:
        logger.info("데이터베이스 초기화 중...")
        create_tables()
        logger.info("데이터베이스 초기화 완료")
    
    # 특정 분석 즉시 실행
    if args.run:
        if args.run == 'all':
            run_all()
        else:
            run_specific_analysis(args.run)
        return
    
    # 스케줄러 실행
    run_scheduler()

if __name__ == "__main__":
    main() 
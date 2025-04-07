import asyncio
import telegram
from config.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, KOSPI_TELEGRAM_BOT_TOKEN, KOSPI_TELEGRAM_CHAT_ID

async def send_message(message, is_kospi=False):
    """
    텔레그램 메시지 전송
    
    Args:
        message: 전송할 메시지 텍스트
        is_kospi: 코스피/코스닥 봇으로 전송할지 여부
    
    Returns:
        성공 여부 (bool)
    """
    try:
        token = KOSPI_TELEGRAM_BOT_TOKEN if is_kospi else TELEGRAM_BOT_TOKEN
        chat_id = KOSPI_TELEGRAM_CHAT_ID if is_kospi else TELEGRAM_CHAT_ID
        
        bot = telegram.Bot(token=token)
        await bot.send_message(
            chat_id=chat_id, 
            text=message, 
            parse_mode='Markdown'
        )
        print(f"텔레그램 메시지 전송 성공: {message[:30]}...")
        return True
    except Exception as e:
        print(f"텔레그램 메시지 전송 오류: {e}")
        return False

# 동기 방식으로 메시지 전송 (다른 코드에서 호출하기 쉽게)
def send_telegram_message(message, is_kospi=False):
    """
    텔레그램 메시지를 동기 방식으로 전송
    
    Args:
        message: 전송할 메시지 텍스트
        is_kospi: 코스피/코스닥 봇으로 전송할지 여부
    
    Returns:
        성공 여부 (bool)
    """
    return asyncio.run(send_message(message, is_kospi))

# 텔레그램 차트 이미지 전송
async def send_image(image_path, caption=None, is_kospi=False):
    """
    텔레그램으로 이미지 전송
    
    Args:
        image_path: 전송할 이미지 파일 경로
        caption: 이미지 캡션
        is_kospi: 코스피/코스닥 봇으로 전송할지 여부
    
    Returns:
        성공 여부 (bool)
    """
    try:
        token = KOSPI_TELEGRAM_BOT_TOKEN if is_kospi else TELEGRAM_BOT_TOKEN
        chat_id = KOSPI_TELEGRAM_CHAT_ID if is_kospi else TELEGRAM_CHAT_ID
        
        bot = telegram.Bot(token=token)
        with open(image_path, 'rb') as photo:
            await bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption,
                parse_mode='Markdown'
            )
        print(f"텔레그램 이미지 전송 성공: {image_path}")
        return True
    except Exception as e:
        print(f"텔레그램 이미지 전송 오류: {e}")
        return False

# 동기 방식으로 이미지 전송
def send_telegram_image(image_path, caption=None, is_kospi=False):
    """
    텔레그램 이미지를 동기 방식으로 전송
    
    Args:
        image_path: 전송할 이미지 파일 경로
        caption: 이미지 캡션
        is_kospi: 코스피/코스닥 봇으로 전송할지 여부
    
    Returns:
        성공 여부 (bool)
    """
    return asyncio.run(send_image(image_path, caption, is_kospi)) 
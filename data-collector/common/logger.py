"""로깅 유틸리티 모듈 - Phase 3 개선"""
import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from .config import LOG_DIR
import os

def get_logger(name, log_file=None, level=logging.INFO):
    """
    로거 생성 함수 (개선된 버전)

    Args:
        name: 로거 이름
        log_file: 로그 파일명 (None이면 파일 로깅 안함)
        level: 로그 레벨 (기본: INFO)

    Returns:
        logging.Logger: 설정된 로거
    """
    logger = logging.getLogger(name)

    # 이미 핸들러가 있으면 반환
    if logger.handlers:
        return logger

    logger.setLevel(level)

    # 포맷터
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 파일 핸들러 (회전 로그)
    if log_file:
        log_path = os.path.join(LOG_DIR, log_file)
        # 5MB 최대, 5개 백업 파일
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 에러 전용 로그 파일
    error_log_path = os.path.join(LOG_DIR, 'errors.log')
    error_handler = RotatingFileHandler(
        error_log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    return logger


def log_exception(logger, message, exc_info=True):
    """
    예외 로깅 헬퍼 함수

    Args:
        logger: 로거 인스턴스
        message: 에러 메시지
        exc_info: 예외 정보 포함 여부
    """
    logger.error(message, exc_info=exc_info)


def log_api_call(logger, api_name, params=None, success=True, error=None):
    """
    API 호출 로깅

    Args:
        logger: 로거 인스턴스
        api_name: API 이름
        params: 요청 파라미터
        success: 성공 여부
        error: 에러 메시지
    """
    if success:
        logger.info(f"API 호출 성공: {api_name} | params: {params}")
    else:
        logger.error(f"API 호출 실패: {api_name} | params: {params} | error: {error}")


def log_db_operation(logger, operation, table, count=None, success=True, error=None):
    """
    데이터베이스 작업 로깅

    Args:
        logger: 로거 인스턴스
        operation: 작업 종류 (INSERT, UPDATE, SELECT 등)
        table: 테이블명
        count: 처리된 행 수
        success: 성공 여부
        error: 에러 메시지
    """
    if success:
        count_str = f"({count}건)" if count is not None else ""
        logger.info(f"DB {operation} 성공: {table} {count_str}")
    else:
        logger.error(f"DB {operation} 실패: {table} | error: {error}")

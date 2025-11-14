const winston = require('winston');
const path = require('path');
const config = require('../config/config');

// 로그 레벨 정의
// error: 0, warn: 1, info: 2, http: 3, debug: 4
const levels = {
  error: 0,
  warn: 1,
  info: 2,
  http: 3,
  debug: 4,
};

// 환경별 로그 레벨
const level = () => {
  const env = config.server.env || 'development';
  const isDevelopment = env === 'development';
  return isDevelopment ? 'debug' : 'info';
};

// 로그 포맷 정의
const format = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json(),
  winston.format.printf((info) => {
    const { timestamp, level, message, stack, ...meta } = info;

    let logMessage = `${timestamp} [${level.toUpperCase()}]: ${message}`;

    // 메타데이터가 있으면 추가
    if (Object.keys(meta).length > 0) {
      logMessage += ` ${JSON.stringify(meta)}`;
    }

    // 스택 트레이스가 있으면 추가
    if (stack) {
      logMessage += `\n${stack}`;
    }

    return logMessage;
  })
);

// 콘솔용 컬러 포맷
const consoleFormat = winston.format.combine(
  winston.format.colorize({ all: true }),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.printf((info) => {
    const { timestamp, level, message, stack, ...meta } = info;

    let logMessage = `${timestamp} ${level}: ${message}`;

    if (Object.keys(meta).length > 0) {
      logMessage += ` ${JSON.stringify(meta, null, 2)}`;
    }

    if (stack) {
      logMessage += `\n${stack}`;
    }

    return logMessage;
  })
);

// 로그 디렉토리 생성
const logsDir = path.join(__dirname, '../../logs');

// 트랜스포트 설정
const transports = [
  // 콘솔 출력
  new winston.transports.Console({
    format: consoleFormat,
  }),

  // 에러 로그 파일
  new winston.transports.File({
    filename: path.join(logsDir, 'error.log'),
    level: 'error',
    format: format,
    maxsize: 5242880, // 5MB
    maxFiles: 5,
  }),

  // 전체 로그 파일
  new winston.transports.File({
    filename: path.join(logsDir, 'combined.log'),
    format: format,
    maxsize: 5242880, // 5MB
    maxFiles: 5,
  }),
];

// 개발 환경에서는 디버그 로그 파일 추가
if (config.server.env === 'development') {
  transports.push(
    new winston.transports.File({
      filename: path.join(logsDir, 'debug.log'),
      level: 'debug',
      format: format,
      maxsize: 5242880, // 5MB
      maxFiles: 3,
    })
  );
}

// Logger 인스턴스 생성
const logger = winston.createLogger({
  level: level(),
  levels,
  format,
  transports,
  exitOnError: false,
});

// Stream 객체 (Morgan과 함께 사용)
logger.stream = {
  write: (message) => {
    logger.http(message.trim());
  },
};

module.exports = logger;

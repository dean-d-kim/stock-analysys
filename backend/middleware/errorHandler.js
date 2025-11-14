const logger = require('../utils/logger');

// 에러 핸들링 미들웨어
const errorHandler = (err, req, res, next) => {
  // 에러 로깅
  logger.error('에러 발생', {
    error: err.message,
    stack: err.stack,
    method: req.method,
    url: req.url,
    ip: req.ip,
    userAgent: req.get('user-agent')
  });

  // 데이터베이스 에러
  if (err.code && err.code.startsWith('23')) {
    return res.status(400).json({
      error: '데이터베이스 제약 조건 위반',
      details: err.message
    });
  }

  // 기본 에러
  const statusCode = err.statusCode || 500;
  res.status(statusCode).json({
    error: err.message || '서버 오류가 발생했습니다.',
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
  });
};

// 404 핸들러
const notFoundHandler = (req, res) => {
  logger.warn('404 - 리소스를 찾을 수 없음', {
    method: req.method,
    url: req.url,
    ip: req.ip
  });

  res.status(404).json({
    error: '요청한 리소스를 찾을 수 없습니다.',
    path: req.path
  });
};

module.exports = { errorHandler, notFoundHandler };

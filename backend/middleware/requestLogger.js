const morgan = require('morgan');
const logger = require('../utils/logger');

// Morgan 토큰 정의
morgan.token('id', (req) => req.id);

// 커스텀 포맷 정의
const morganFormat = ':method :url :status :response-time ms - :res[content-length]';

// Morgan 미들웨어 설정
const requestLogger = morgan(morganFormat, {
  stream: logger.stream,
  skip: (req, res) => {
    // Health check 엔드포인트는 로그에서 제외
    return req.url === '/health' || req.url === '/';
  },
});

module.exports = requestLogger;

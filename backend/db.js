const { Pool } = require('pg');
const config = require('./config/config');
const logger = require('./utils/logger');

// 연결 풀 설정 최적화
const pool = new Pool({
  ...config.database,
  max: 20, // 최대 연결 수
  idleTimeoutMillis: 30000, // 유휴 연결 타임아웃 (30초)
  connectionTimeoutMillis: 5000, // 연결 타임아웃 (5초)
  maxUses: 7500, // 연결 재사용 최대 횟수
});

// 연결 테스트
pool.on('connect', () => {
  logger.info('데이터베이스 연결 성공');
});

pool.on('error', (err) => {
  logger.error('데이터베이스 연결 오류', { error: err.message, stack: err.stack });
  process.exit(-1);
});

module.exports = pool;

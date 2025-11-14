// 간단한 메모리 캐싱 미들웨어
// 종목 목록과 같이 자주 조회되지만 자주 변경되지 않는 데이터에 사용

const logger = require('../utils/logger');

const cache = new Map();
const CACHE_DURATION = 5 * 60 * 1000; // 5분

/**
 * 캐시 미들웨어
 * @param {number} duration - 캐시 유지 시간 (밀리초), 기본값 5분
 */
function cacheMiddleware(duration = CACHE_DURATION) {
  return (req, res, next) => {
    // GET 요청만 캐시
    if (req.method !== 'GET') {
      return next();
    }

    const key = req.originalUrl || req.url;
    const cachedResponse = cache.get(key);

    if (cachedResponse) {
      const { data, timestamp } = cachedResponse;
      const age = Date.now() - timestamp;

      // 캐시가 유효한 경우
      if (age < duration) {
        logger.debug(`캐시 히트: ${key} (${Math.round(age / 1000)}초 전)`);
        res.setHeader('X-Cache', 'HIT');
        res.setHeader('X-Cache-Age', Math.round(age / 1000));
        return res.json(data);
      }

      // 캐시가 만료된 경우 삭제
      cache.delete(key);
      logger.debug(`캐시 만료: ${key}`);
    }

    // 응답을 캐시에 저장
    const originalJson = res.json.bind(res);
    res.json = (data) => {
      cache.set(key, {
        data,
        timestamp: Date.now()
      });

      res.setHeader('X-Cache', 'MISS');
      logger.debug(`캐시 저장: ${key}`);

      return originalJson(data);
    };

    next();
  };
}

/**
 * 특정 키의 캐시 삭제
 */
function clearCache(key) {
  if (key) {
    cache.delete(key);
    logger.info(`캐시 삭제: ${key}`);
  } else {
    cache.clear();
    logger.info('전체 캐시 삭제');
  }
}

/**
 * 캐시 상태 조회
 */
function getCacheStats() {
  return {
    size: cache.size,
    keys: Array.from(cache.keys())
  };
}

module.exports = {
  cacheMiddleware,
  clearCache,
  getCacheStats,
  CACHE_DURATION
};

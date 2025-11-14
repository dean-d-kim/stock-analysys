const express = require('express');
const router = express.Router();
const fs = require('fs').promises;
const path = require('path');
const { Pool } = require('pg');
const config = require('../config/config');
const logger = require('../utils/logger');
const { getCacheStats, clearCache } = require('../middleware/cache');

/**
 * @swagger
 * /api/admin/health:
 *   get:
 *     summary: 서버 헬스 체크
 *     description: 서버 상태, 데이터베이스 연결, 메모리 사용량 등을 확인합니다.
 *     tags: [Admin]
 *     responses:
 *       200:
 *         description: 서버 상태 정보
 */
router.get('/health', async (req, res) => {
  try {
    const pool = new Pool(config.database);

    // DB 연결 테스트
    let dbStatus = 'disconnected';
    let dbError = null;
    try {
      await pool.query('SELECT 1');
      dbStatus = 'connected';
    } catch (err) {
      dbError = err.message;
    } finally {
      await pool.end();
    }

    // 메모리 사용량
    const memoryUsage = process.memoryUsage();

    // 업타임
    const uptime = process.uptime();

    // 캐시 통계
    const cacheStats = getCacheStats();

    const healthInfo = {
      status: dbStatus === 'connected' ? 'healthy' : 'unhealthy',
      timestamp: new Date().toISOString(),
      uptime: {
        seconds: uptime,
        formatted: formatUptime(uptime)
      },
      database: {
        status: dbStatus,
        error: dbError,
        host: config.database.host,
        port: config.database.port,
        database: config.database.database
      },
      memory: {
        rss: `${(memoryUsage.rss / 1024 / 1024).toFixed(2)} MB`,
        heapTotal: `${(memoryUsage.heapTotal / 1024 / 1024).toFixed(2)} MB`,
        heapUsed: `${(memoryUsage.heapUsed / 1024 / 1024).toFixed(2)} MB`,
        external: `${(memoryUsage.external / 1024 / 1024).toFixed(2)} MB`
      },
      cache: {
        size: cacheStats.size,
        keys: cacheStats.keys
      },
      environment: config.server.env,
      nodeVersion: process.version,
      platform: process.platform
    };

    logger.info('헬스 체크 요청', { status: healthInfo.status });
    res.json(healthInfo);
  } catch (err) {
    logger.error('헬스 체크 실패', { error: err.message, stack: err.stack });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/admin/logs:
 *   get:
 *     summary: 로그 파일 목록 조회
 *     description: 사용 가능한 로그 파일 목록을 조회합니다.
 *     tags: [Admin]
 *     responses:
 *       200:
 *         description: 로그 파일 목록
 */
router.get('/logs', async (req, res) => {
  try {
    const logsDir = path.join(__dirname, '../../logs');
    const files = await fs.readdir(logsDir);

    const logFiles = await Promise.all(
      files
        .filter(file => file.endsWith('.log'))
        .map(async (file) => {
          const filePath = path.join(logsDir, file);
          const stats = await fs.stat(filePath);
          return {
            name: file,
            size: stats.size,
            sizeFormatted: formatBytes(stats.size),
            modified: stats.mtime,
            created: stats.birthtime
          };
        })
    );

    // 수정 시간 기준 내림차순 정렬
    logFiles.sort((a, b) => b.modified - a.modified);

    logger.info('로그 파일 목록 조회');
    res.json(logFiles);
  } catch (err) {
    logger.error('로그 파일 목록 조회 실패', { error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/admin/logs/{filename}:
 *   get:
 *     summary: 로그 파일 내용 조회
 *     description: 특정 로그 파일의 내용을 조회합니다.
 *     tags: [Admin]
 *     parameters:
 *       - name: filename
 *         in: path
 *         required: true
 *         schema:
 *           type: string
 *       - name: lines
 *         in: query
 *         schema:
 *           type: integer
 *           default: 100
 *     responses:
 *       200:
 *         description: 로그 파일 내용
 */
router.get('/logs/:filename', async (req, res) => {
  try {
    const { filename } = req.params;
    const lines = parseInt(req.query.lines) || 100;

    // 보안: 경로 탐색 방지
    if (filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: '유효하지 않은 파일명입니다.' });
    }

    const logsDir = path.join(__dirname, '../../logs');
    const filePath = path.join(logsDir, filename);

    // 파일 존재 확인
    try {
      await fs.access(filePath);
    } catch {
      return res.status(404).json({ error: '로그 파일을 찾을 수 없습니다.' });
    }

    const content = await fs.readFile(filePath, 'utf-8');
    const allLines = content.split('\n');
    const recentLines = allLines.slice(-lines);

    logger.info('로그 파일 조회', { filename, lines: recentLines.length });
    res.json({
      filename,
      lines: recentLines,
      totalLines: allLines.length,
      displayedLines: recentLines.length
    });
  } catch (err) {
    logger.error('로그 파일 조회 실패', { error: err.message, filename: req.params.filename });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/admin/cache:
 *   delete:
 *     summary: 캐시 삭제
 *     description: 전체 또는 특정 키의 캐시를 삭제합니다.
 *     tags: [Admin]
 *     parameters:
 *       - name: key
 *         in: query
 *         schema:
 *           type: string
 *     responses:
 *       200:
 *         description: 캐시 삭제 성공
 */
router.delete('/cache', (req, res) => {
  try {
    const { key } = req.query;

    if (key) {
      clearCache(key);
      logger.info('캐시 삭제 (키)', { key });
      res.json({ message: `캐시 삭제 완료: ${key}` });
    } else {
      clearCache();
      logger.info('전체 캐시 삭제');
      res.json({ message: '전체 캐시 삭제 완료' });
    }
  } catch (err) {
    logger.error('캐시 삭제 실패', { error: err.message });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/admin/stats/db:
 *   get:
 *     summary: 데이터베이스 통계
 *     description: 종목 수, 일별 시세 데이터 수 등의 통계를 조회합니다.
 *     tags: [Admin]
 *     responses:
 *       200:
 *         description: 데이터베이스 통계
 */
router.get('/stats/db', async (req, res) => {
  const pool = new Pool(config.database);

  try {
    // 종목 수
    const stocksCount = await pool.query('SELECT COUNT(*) FROM stocks');

    // 시장 유형별 종목 수
    const stocksByMarket = await pool.query(`
      SELECT market_type, COUNT(*) as count
      FROM stocks
      GROUP BY market_type
      ORDER BY count DESC
    `);

    // 일별 시세 데이터 수
    const dailyPricesCount = await pool.query('SELECT COUNT(*) FROM daily_prices');

    // 최신 데이터 날짜
    const latestDate = await pool.query(`
      SELECT MAX(trade_date) as latest_date
      FROM daily_prices
    `);

    // 가장 오래된 데이터 날짜
    const oldestDate = await pool.query(`
      SELECT MIN(trade_date) as oldest_date
      FROM daily_prices
    `);

    // 테이블 크기
    const tableSize = await pool.query(`
      SELECT
        pg_size_pretty(pg_total_relation_size('stocks')) as stocks_size,
        pg_size_pretty(pg_total_relation_size('daily_prices')) as daily_prices_size
    `);

    const stats = {
      stocks: {
        total: parseInt(stocksCount.rows[0].count),
        byMarket: stocksByMarket.rows
      },
      dailyPrices: {
        total: parseInt(dailyPricesCount.rows[0].count),
        latestDate: latestDate.rows[0].latest_date,
        oldestDate: oldestDate.rows[0].oldest_date
      },
      tableSize: tableSize.rows[0]
    };

    logger.info('데이터베이스 통계 조회');
    res.json(stats);
  } catch (err) {
    logger.error('데이터베이스 통계 조회 실패', { error: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// 데이터 범위 조회 (시작일, 최근 적재일)
router.get('/stats/data-range', async (req, res) => {
  const pool = new Pool(config.database);

  try {
    // 각 자산 유형별 데이터 범위 조회
    const query = `
      SELECT
        s.asset_type,
        COUNT(DISTINCT s.stock_code) as stock_count,
        MIN(dp.trade_date) as start_date,
        MAX(dp.trade_date) as latest_date,
        COUNT(*) as total_records
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type IN ('STOCK', 'ETF')
      GROUP BY s.asset_type
      ORDER BY s.asset_type
    `;

    const result = await pool.query(query);

    // KOSPI/KOSDAQ 구분을 위한 추가 쿼리
    const marketQuery = `
      SELECT
        s.market_type,
        COUNT(DISTINCT s.stock_code) as stock_count,
        MIN(dp.trade_date) as start_date,
        MAX(dp.trade_date) as latest_date,
        COUNT(*) as total_records
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type = 'STOCK' AND s.market_type IN ('KOSPI', 'KOSDAQ')
      GROUP BY s.market_type
      ORDER BY s.market_type
    `;

    const marketResult = await pool.query(marketQuery);

    // ETF 데이터
    const etfQuery = `
      SELECT
        s.asset_type,
        COUNT(DISTINCT s.stock_code) as stock_count,
        MIN(dp.trade_date) as start_date,
        MAX(dp.trade_date) as latest_date,
        COUNT(*) as total_records
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type = 'ETF'
      GROUP BY s.asset_type
    `;

    const etfResult = await pool.query(etfQuery);

    logger.info('데이터 범위 조회');
    res.json({
      markets: marketResult.rows,
      etf: etfResult.rows[0] || null,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    logger.error('데이터 범위 조회 실패', { error: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// 데이터 누락 현황 조회 (컬럼별)
router.get('/stats/missing-data', async (req, res) => {
  const pool = new Pool(config.database);

  try {
    // 각 자산 유형별 컬럼 누락 현황 조회
    const query = `
      SELECT
        s.asset_type,
        COUNT(*) as total_records,
        COUNT(dp.open_price) as has_open_price,
        COUNT(*) - COUNT(dp.open_price) as missing_open_price,
        COUNT(dp.high_price) as has_high_price,
        COUNT(*) - COUNT(dp.high_price) as missing_high_price,
        COUNT(dp.low_price) as has_low_price,
        COUNT(*) - COUNT(dp.low_price) as missing_low_price,
        COUNT(dp.close_price) as has_close_price,
        COUNT(*) - COUNT(dp.close_price) as missing_close_price,
        COUNT(dp.volume) as has_volume,
        COUNT(*) - COUNT(dp.volume) as missing_volume,
        COUNT(dp.vs) as has_vs,
        COUNT(*) - COUNT(dp.vs) as missing_vs,
        COUNT(dp.change_rate) as has_change_rate,
        COUNT(*) - COUNT(dp.change_rate) as missing_change_rate,
        COUNT(dp.trading_value) as has_trading_value,
        COUNT(*) - COUNT(dp.trading_value) as missing_trading_value
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type IN ('STOCK', 'ETF')
      GROUP BY s.asset_type
      ORDER BY s.asset_type
    `;

    const result = await pool.query(query);

    // KOSPI/KOSDAQ 구분
    const marketQuery = `
      SELECT
        s.market_type,
        COUNT(*) as total_records,
        COUNT(dp.open_price) as has_open_price,
        COUNT(*) - COUNT(dp.open_price) as missing_open_price,
        COUNT(dp.high_price) as has_high_price,
        COUNT(*) - COUNT(dp.high_price) as missing_high_price,
        COUNT(dp.low_price) as has_low_price,
        COUNT(*) - COUNT(dp.low_price) as missing_low_price,
        COUNT(dp.close_price) as has_close_price,
        COUNT(*) - COUNT(dp.close_price) as missing_close_price,
        COUNT(dp.volume) as has_volume,
        COUNT(*) - COUNT(dp.volume) as missing_volume,
        COUNT(dp.vs) as has_vs,
        COUNT(*) - COUNT(dp.vs) as missing_vs,
        COUNT(dp.change_rate) as has_change_rate,
        COUNT(*) - COUNT(dp.change_rate) as missing_change_rate,
        COUNT(dp.trading_value) as has_trading_value,
        COUNT(*) - COUNT(dp.trading_value) as missing_trading_value
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type = 'STOCK' AND s.market_type IN ('KOSPI', 'KOSDAQ')
      GROUP BY s.market_type
      ORDER BY s.market_type
    `;

    const marketResult = await pool.query(marketQuery);

    // ETF 데이터
    const etfQuery = `
      SELECT
        s.asset_type,
        COUNT(*) as total_records,
        COUNT(dp.open_price) as has_open_price,
        COUNT(*) - COUNT(dp.open_price) as missing_open_price,
        COUNT(dp.high_price) as has_high_price,
        COUNT(*) - COUNT(dp.high_price) as missing_high_price,
        COUNT(dp.low_price) as has_low_price,
        COUNT(*) - COUNT(dp.low_price) as missing_low_price,
        COUNT(dp.close_price) as has_close_price,
        COUNT(*) - COUNT(dp.close_price) as missing_close_price,
        COUNT(dp.volume) as has_volume,
        COUNT(*) - COUNT(dp.volume) as missing_volume,
        COUNT(dp.vs) as has_vs,
        COUNT(*) - COUNT(dp.vs) as missing_vs,
        COUNT(dp.change_rate) as has_change_rate,
        COUNT(*) - COUNT(dp.change_rate) as missing_change_rate,
        COUNT(dp.trading_value) as has_trading_value,
        COUNT(*) - COUNT(dp.trading_value) as missing_trading_value
      FROM stocks s
      LEFT JOIN daily_prices dp ON s.stock_code = dp.stock_code
      WHERE s.asset_type = 'ETF'
      GROUP BY s.asset_type
    `;

    const etfResult = await pool.query(etfQuery);

    logger.info('데이터 누락 현황 조회');
    res.json({
      markets: marketResult.rows,
      etf: etfResult.rows[0] || null,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    logger.error('데이터 누락 현황 조회 실패', { error: err.message });
    res.status(500).json({ error: err.message });
  } finally {
    await pool.end();
  }
});

// 헬퍼 함수
function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);

  return `${days}d ${hours}h ${minutes}m ${secs}s`;
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

module.exports = router;

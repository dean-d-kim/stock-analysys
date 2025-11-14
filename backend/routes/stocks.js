const express = require('express');
const router = express.Router();
const pool = require('../db');
const logger = require('../utils/logger');
const { findOne, findMany } = require('../utils/dbHelper');
const {
  validateStockList,
  validateStockSearch,
  validateStockCode,
  validateDailyPrices
} = require('../middleware/validator');
const { cacheMiddleware } = require('../middleware/cache');

/**
 * @swagger
 * /api/stocks:
 *   get:
 *     summary: 종목 목록 조회
 *     description: 시가총액 또는 거래대금 기준으로 정렬된 종목 목록을 조회합니다. (5분 캐싱)
 *     tags: [Stocks]
 *     parameters:
 *       - $ref: '#/components/parameters/marketType'
 *     responses:
 *       200:
 *         description: 성공
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Stock'
 *       400:
 *         $ref: '#/components/responses/BadRequest'
 *       500:
 *         $ref: '#/components/responses/ServerError'
 */
router.get('/', validateStockList, cacheMiddleware(5 * 60 * 1000), async (req, res) => {
  try {
    const { market_type } = req.query;

    // 단일 쿼리로 최적화: 최근 2일 가격 데이터를 윈도우 함수로 조회
    let query = `
      WITH latest_prices AS (
        SELECT
          stock_code,
          close_price,
          volume,
          trade_date,
          LAG(close_price) OVER (PARTITION BY stock_code ORDER BY trade_date DESC) as prev_close_price,
          ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY trade_date DESC) as rn
        FROM daily_prices
      )
      SELECT
        s.*,
        lp.close_price as current_price,
        lp.volume,
        to_char(lp.trade_date, 'YYYY-MM-DD') as last_update_date,
        (lp.close_price * lp.volume) as trading_value,
        (lp.close_price * s.listed_shares) as calculated_market_cap,
        CASE
          WHEN lp.prev_close_price IS NOT NULL AND lp.prev_close_price != 0
          THEN ((lp.close_price - lp.prev_close_price)::numeric / lp.prev_close_price * 100)
          ELSE 0
        END as change_rate
      FROM stocks s
      LEFT JOIN latest_prices lp ON s.stock_code = lp.stock_code AND lp.rn = 1
    `;

    let params = [];

    if (market_type) {
      query += ' WHERE s.market_type = $1';
      params.push(market_type);
    } else {
      // market_type이 지정되지 않은 경우 KONEX 제외
      query += ' WHERE s.market_type != $1';
      params.push('KONEX');
    }

    // ETF는 거래대금 기준, KOSPI/KOSDAQ은 계산된 시가총액 기준 정렬
    if (market_type === 'ETF') {
      query += ' ORDER BY trading_value DESC NULLS LAST, s.stock_code';
    } else {
      query += ' ORDER BY calculated_market_cap DESC NULLS LAST, s.stock_code';
    }

    const result = await pool.query(query, params);

    // volume을 정수로 변환하고 market_cap 설정
    const stocks = result.rows.map(stock => ({
      ...stock,
      volume: stock.volume ? parseInt(stock.volume) : null,
      change_rate: stock.change_rate ? parseFloat(stock.change_rate) : 0,
      market_cap: stock.calculated_market_cap || stock.market_cap
    }));

    res.json(stocks);
  } catch (err) {
    logger.error('종목 목록 조회 실패', { error: err.message, stack: err.stack, market_type: req.query.market_type });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/stocks/search:
 *   get:
 *     summary: 종목 검색
 *     description: 종목명 또는 종목코드로 종목을 검색합니다.
 *     tags: [Stocks]
 *     parameters:
 *       - $ref: '#/components/parameters/searchQuery'
 *     responses:
 *       200:
 *         description: 성공
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 type: object
 *                 properties:
 *                   stock_code:
 *                     type: string
 *                   stock_name:
 *                     type: string
 *                   market_type:
 *                     type: string
 *       400:
 *         $ref: '#/components/responses/BadRequest'
 *       500:
 *         $ref: '#/components/responses/ServerError'
 */
router.get('/search', validateStockSearch, async (req, res) => {
  try {
    const { q } = req.query;

    if (!q || q.trim().length === 0) {
      return res.json([]);
    }

    const stocks = await findMany(
      `SELECT stock_code, stock_name, market_type
       FROM stocks
       WHERE stock_name LIKE $1 OR stock_code LIKE $1
       ORDER BY market_cap DESC NULLS LAST
       LIMIT 20`,
      [`%${q}%`],
      '종목 검색'
    );

    res.json(stocks);
  } catch (err) {
    logger.error('종목 검색 실패', { error: err.message, stack: err.stack, query: req.query.q });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/stocks/{code}:
 *   get:
 *     summary: 종목 상세 정보 조회
 *     description: 특정 종목의 상세 정보를 조회합니다.
 *     tags: [Stocks]
 *     parameters:
 *       - $ref: '#/components/parameters/stockCode'
 *     responses:
 *       200:
 *         description: 성공
 *         content:
 *           application/json:
 *             schema:
 *               $ref: '#/components/schemas/Stock'
 *       404:
 *         $ref: '#/components/responses/NotFound'
 *       500:
 *         $ref: '#/components/responses/ServerError'
 */
router.get('/:code', validateStockCode, async (req, res) => {
  try {
    const { code } = req.params;
    const stock = await findOne(
      'SELECT * FROM stocks WHERE stock_code = $1',
      [code],
      '종목 상세 조회'
    );

    if (!stock) {
      return res.status(404).json({ error: '종목을 찾을 수 없습니다.' });
    }

    res.json(stock);
  } catch (err) {
    logger.error('종목 상세 조회 실패', { error: err.message, stack: err.stack, stock_code: req.params.code });
    res.status(500).json({ error: err.message });
  }
});

/**
 * @swagger
 * /api/stocks/{code}/daily:
 *   get:
 *     summary: 일별 시세 조회
 *     description: 특정 종목의 일별 시세 데이터를 조회합니다.
 *     tags: [Stocks]
 *     parameters:
 *       - $ref: '#/components/parameters/stockCode'
 *       - $ref: '#/components/parameters/limit'
 *     responses:
 *       200:
 *         description: 성공
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/DailyPrice'
 *       400:
 *         $ref: '#/components/responses/BadRequest'
 *       500:
 *         $ref: '#/components/responses/ServerError'
 */
router.get('/:code/daily', validateDailyPrices, async (req, res) => {
  try {
    const { code } = req.params;
    const { limit = 365 } = req.query;

    const prices = await findMany(
      `SELECT id, stock_code,
              to_char(trade_date, 'YYYY-MM-DD') as trade_date,
              open_price, high_price, low_price, close_price,
              volume, trading_value, created_at, vs, change_rate
       FROM daily_prices
       WHERE stock_code = $1
       ORDER BY trade_date DESC
       LIMIT $2`,
      [code, parseInt(limit)],
      '일별 시세 조회'
    );

    res.json(prices);
  } catch (err) {
    logger.error('일별 시세 조회 실패', { error: err.message, stack: err.stack, stock_code: req.params.code, limit: req.query.limit });
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;

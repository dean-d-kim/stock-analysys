const express = require('express');
const cors = require('cors');
const pool = require('./db');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.json());

// 종목 목록
app.get('/api/stocks', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM stocks ORDER BY stock_code');
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 종목 상세
app.get('/api/stocks/:code', async (req, res) => {
  try {
    const { code } = req.params;
    const result = await pool.query('SELECT * FROM stocks WHERE stock_code = $1', [code]);
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// 일별 시세
app.get('/api/stocks/:code/daily', async (req, res) => {
  try {
    const { code } = req.params;
    const result = await pool.query(
      'SELECT * FROM daily_prices WHERE stock_code = $1 ORDER BY trade_date DESC LIMIT 100',
      [code]
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`✅ 서버 실행: http://localhost:${PORT}`);
});

// 실시간 시세
app.get('/api/stocks/:code/realtime', async (req, res) => {
  try {
    const { code } = req.params;
    const result = await pool.query(
      'SELECT * FROM realtime_prices WHERE stock_code = $1 ORDER BY timestamp DESC LIMIT 1',
      [code]
    );
    res.json(result.rows[0]);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

import React, { useState, useEffect } from 'react';
import axios from 'axios';
import StockChart from './StockChart';
import './App.css';

function App() {
  const [stocks, setStocks] = useState([]);
  const [realtimeData, setRealtimeData] = useState({});
  const [selectedStock, setSelectedStock] = useState('005930');

  useEffect(() => {
    axios.get('/api/stocks')
      .then(res => setStocks(res.data));

    const interval = setInterval(() => {
      stocks.forEach(stock => {
        axios.get(`/api/stocks/${stock.stock_code}/realtime`)
          .then(res => {
            setRealtimeData(prev => ({
              ...prev,
              [stock.stock_code]: res.data
            }));
          });
      });
    }, 5000);

    return () => clearInterval(interval);
  }, [stocks]);

  return (
    <div className="App">
      <h1>📈 주식 분석 시스템</h1>
      
      <div className="chart-container">
        <h2>{selectedStock} 차트</h2>
        <StockChart stockCode={selectedStock} />
      </div>

      <table>
        <thead>
          <tr>
            <th>종목코드</th>
            <th>종목명</th>
            <th>현재가</th>
            <th>거래량</th>
          </tr>
        </thead>
        <tbody>
          {stocks.map(stock => (
            <tr key={stock.stock_code} onClick={() => setSelectedStock(stock.stock_code)}>
              <td>{stock.stock_code}</td>
              <td>{stock.stock_name}</td>
              <td>{realtimeData[stock.stock_code]?.current_price?.toLocaleString()}원</td>
              <td>{realtimeData[stock.stock_code]?.volume?.toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;

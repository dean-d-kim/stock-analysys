import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import axios from 'axios';

function StockChart({ stockCode }) {
  const [data, setData] = useState([]);

  useEffect(() => {
    axios.get(`http://localhost:3000/api/stocks/${stockCode}/daily`)
      .then(res => {
        const formatted = res.data.reverse().map(item => ({
          date: item.trade_date.substring(5, 10),
          price: item.close_price
        }));
        setData(formatted);
      });
  }, [stockCode]);

  return (
    <ResponsiveContainer width="100%" height={300}>
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="price" stroke="#4CAF50" name="종가" />
      </LineChart>
    </ResponsiveContainer>
  );
}

export default StockChart;

import React, { useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import { stockAPI } from './services/api';
import './StockComparison.css';

// Chart.js 컴포넌트 등록
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

function StockComparison({ selectedStocks, onClose }) {
  const [comparisonData, setComparisonData] = useState(null);
  const [period, setPeriod] = useState(30); // 기본 30일
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (selectedStocks.length > 0) {
      fetchComparisonData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedStocks, period]);

  const fetchComparisonData = async () => {
    setLoading(true);
    setError(null);

    // 각 종목의 일별 데이터 가져오기
    const promises = selectedStocks.map(stock =>
      stockAPI.getDailyPrices(stock.stock_code)
    );

    const results = await Promise.all(promises);

    // 에러 체크
    const hasError = results.some(result => result.error);
    if (hasError) {
      const errorResult = results.find(result => result.error);
      setError(errorResult.error);
      setLoading(false);
      return;
    }

    // 상승률 계산
    const processedData = results.map((result, idx) => {
      const prices = result.data.slice(0, period).reverse(); // 최근 N일 데이터

      if (prices.length === 0) return null;

      const basePrice = prices[0].close_price;

      return {
        stockCode: selectedStocks[idx].stock_code,
        stockName: selectedStocks[idx].stock_name,
        data: prices.map(p => ({
          date: p.trade_date,
          returnRate: ((p.close_price - basePrice) / basePrice) * 100
        }))
      };
    }).filter(d => d !== null);

    setComparisonData(processedData);
    setLoading(false);
  };

  if (error) {
    return (
      <div className="comparison-overlay" onClick={onClose}>
        <div className="comparison-modal" onClick={(e) => e.stopPropagation()}>
          <button className="close-button" onClick={onClose}>×</button>
          <div style={{
            padding: '15px',
            margin: '10px 0',
            backgroundColor: '#fee',
            border: '1px solid #fcc',
            borderRadius: '4px',
            color: '#c33'
          }}>
            <strong>오류:</strong> {error}
          </div>
        </div>
      </div>
    );
  }

  if (loading || !comparisonData || comparisonData.length === 0) {
    return <div className="comparison-loading">데이터 로딩 중...</div>;
  }

  // 차트 데이터 구성
  const labels = comparisonData[0].data.map(d => {
    const date = new Date(d.date);
    return `${date.getMonth() + 1}/${date.getDate()}`;
  });

  const colors = [
    'rgb(255, 99, 132)',
    'rgb(54, 162, 235)',
    'rgb(255, 206, 86)',
    'rgb(75, 192, 192)',
    'rgb(153, 102, 255)',
    'rgb(255, 159, 64)',
    'rgb(201, 203, 207)',
    'rgb(255, 99, 71)',
    'rgb(144, 238, 144)',
    'rgb(173, 216, 230)'
  ];

  const chartData = {
    labels: labels,
    datasets: comparisonData.map((stock, idx) => ({
      label: stock.stockName,
      data: stock.data.map(d => d.returnRate),
      borderColor: colors[idx % colors.length],
      backgroundColor: colors[idx % colors.length].replace('rgb', 'rgba').replace(')', ', 0.1)'),
      borderWidth: 2,
      tension: 0.1,
      pointRadius: 1,
      pointHoverRadius: 5
    }))
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
        onClick: (e, legendItem, legend) => {
          const index = legendItem.datasetIndex;
          const chart = legend.chart;

          // 현재 클릭한 항목이 이미 단독으로 표시 중인지 확인
          const alreadyIsolated = chart.data.datasets.every((dataset, i) =>
            i === index ? dataset.hidden === false : dataset.hidden === true
          );

          if (alreadyIsolated) {
            // 이미 단독 표시 중이면 모두 표시
            chart.data.datasets.forEach(dataset => {
              dataset.hidden = false;
            });
          } else {
            // 클릭한 항목만 표시하고 나머지는 숨김
            chart.data.datasets.forEach((dataset, i) => {
              dataset.hidden = i !== index;
            });
          }

          chart.update();
        }
      },
      title: {
        display: true,
        text: '종목 수익률 비교 (%) - 범례 클릭으로 개별 종목 선택',
        font: {
          size: 16
        }
      },
      tooltip: {
        mode: 'index',
        intersect: false,
        callbacks: {
          label: function(context) {
            return `${context.dataset.label}: ${context.parsed.y.toFixed(2)}%`;
          }
        }
      }
    },
    scales: {
      y: {
        title: {
          display: true,
          text: '수익률 (%)'
        },
        ticks: {
          callback: function(value) {
            return value.toFixed(1) + '%';
          }
        },
        grid: {
          color: 'rgba(0, 0, 0, 0.05)'
        }
      },
      x: {
        title: {
          display: true,
          text: '날짜'
        },
        ticks: {
          maxTicksLimit: 10
        }
      }
    },
    interaction: {
      mode: 'nearest',
      axis: 'x',
      intersect: false
    }
  };

  return (
    <div className="comparison-container">
      <div className="comparison-header">
        <h2>종목 비교 ({selectedStocks.length}개 종목)</h2>
        <button className="close-button" onClick={onClose}>✕</button>
      </div>

      <div className="period-selector">
        <label>비교 기간:</label>
        <select value={period} onChange={(e) => setPeriod(Number(e.target.value))}>
          <option value={7}>1주일</option>
          <option value={30}>1개월</option>
          <option value={60}>2개월</option>
          <option value={90}>3개월</option>
          <option value={180}>6개월</option>
        </select>
      </div>

      <div className="comparison-chart">
        <Line data={chartData} options={options} />
      </div>

      <div className="comparison-summary">
        <h3>수익률 요약 ({period}일 기준)</h3>
        <table>
          <thead>
            <tr>
              <th>종목명</th>
              <th>종목코드</th>
              <th>수익률</th>
            </tr>
          </thead>
          <tbody>
            {comparisonData.map((stock, idx) => {
              const finalReturn = stock.data[stock.data.length - 1].returnRate;
              const returnClass = finalReturn >= 0 ? 'positive' : 'negative';

              return (
                <tr key={stock.stockCode}>
                  <td>{stock.stockName}</td>
                  <td>{stock.stockCode}</td>
                  <td className={returnClass}>
                    {finalReturn >= 0 ? '+' : ''}{finalReturn.toFixed(2)}%
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default StockComparison;

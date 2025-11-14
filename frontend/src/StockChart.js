import React, { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  TimeScale,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import { CandlestickController, CandlestickElement } from 'chartjs-chart-financial';
import 'chartjs-adapter-date-fns';
import { stockAPI } from './services/api';

// Chart.js에 Candlestick 차트 등록
ChartJS.register(
  CategoryScale,
  LinearScale,
  TimeScale,
  CandlestickController,
  CandlestickElement,
  Title,
  Tooltip,
  Legend
);

function StockChart({ stockCode }) {
  const [period, setPeriod] = useState('3M'); // 기본값: 3개월
  const [customMode, setCustomMode] = useState(false); // 사용자 지정 날짜 모드
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const canvasRef = React.useRef(null);
  const chartRef = React.useRef(null);

  // 기간별 데이터 개수 계산
  const getDataLimit = (period) => {
    switch(period) {
      case '1M': return 20;    // 1개월
      case '3M': return 60;    // 3개월
      case '6M': return 120;   // 6개월
      case '1Y': return 250;   // 1년
      case '3Y': return 750;   // 3년
      case 'ALL': return 10000; // 전체 (2020년부터 ~5년)
      default: return 60;
    }
  };

  // 날짜 범위로 데이터 필터링
  const filterDataByDateRange = (data, start, end) => {
    if (!start && !end) return data;

    return data.filter(item => {
      const itemDate = new Date(item.trade_date);
      const startCheck = start ? itemDate >= new Date(start) : true;
      const endCheck = end ? itemDate <= new Date(end) : true;
      return startCheck && endCheck;
    });
  };

  useEffect(() => {
    if (!stockCode) return;

    // 종목 정보와 일별 데이터를 동시에 가져오기
    const fetchData = async () => {
      setLoading(true);
      setError(null);

      const [stockResult, dailyResult] = await Promise.all([
        stockAPI.getStockDetail(stockCode),
        stockAPI.getDailyPrices(stockCode, 10000)
      ]);

      if (stockResult.error || dailyResult.error) {
        setError(stockResult.error || dailyResult.error);
        setLoading(false);
        return;
      }

      const stockName = stockResult.data.stock_name || stockCode;
      const reversedData = dailyResult.data.reverse();

      let data;
      if (customMode && (startDate || endDate)) {
        // 사용자 지정 날짜 범위
        data = filterDataByDateRange(reversedData, startDate, endDate);
      } else {
        // 기본 기간 선택
        const limit = getDataLimit(period);
        data = reversedData.slice(-limit);
      }

      // 캔들스틱 데이터 생성 (연속적으로 표시하기 위해 인덱스 사용)
      const tradeDates = data.map(item => item.trade_date);
      const candlestickData = data.map((item, index) => ({
        x: index,
        o: item.open_price,
        h: item.high_price,
        l: item.low_price,
        c: item.close_price,
        date: item.trade_date
      }));

      // 거래량 데이터 생성
      const volumeData = data.map((item, index) => ({
        x: index,
        y: item.volume,
        date: item.trade_date,
        color: item.close_price >= item.open_price ? '#ff000080' : '#0000ff80' // 상승: 빨강, 하락: 파랑 (투명도 50%)
      }));

      // 기존 차트 제거 (캔버스를 재사용하기 위해)
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }

      // 캔버스가 존재하는지 확인
      if (!canvasRef.current) {
        setLoading(false);
        return;
      }

      // 새 차트 생성 (Candlestick 차트 + 거래량)
      const ctx = canvasRef.current.getContext('2d');
      chartRef.current = new ChartJS(ctx, {
        type: 'candlestick',
        data: {
          datasets: [
            {
              label: stockName,
              type: 'candlestick',
              data: candlestickData,
              yAxisID: 'y-price',
              color: {
                up: '#ff0000',      // 상승: 빨강
                down: '#0000ff',    // 하락: 파랑
                unchanged: '#999999' // 보합: 회색
              },
              borderColor: {
                up: '#ff0000',
                down: '#0000ff',
                unchanged: '#999999'
              }
            },
            {
              label: '거래량',
              type: 'bar',
              data: volumeData,
              yAxisID: 'y-volume',
              backgroundColor: volumeData.map(item => item.color),
              barThickness: 'flex',
              maxBarThickness: 8
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          parsing: false,
          scales: {
            x: {
              type: 'linear',
              title: {
                display: true,
                text: '날짜'
              },
              ticks: {
                maxRotation: 45,
                minRotation: 45,
                autoSkip: true,
                maxTicksLimit: 20,
                callback: function(value, index) {
                  // 인덱스를 실제 날짜로 변환하여 표시
                  if (tradeDates[value]) {
                    const date = new Date(tradeDates[value]);
                    return `${date.getMonth() + 1}/${date.getDate()}`;
                  }
                  return '';
                }
              }
            },
            'y-price': {
              type: 'linear',
              position: 'left',
              title: {
                display: true,
                text: '가격 (원)'
              },
              ticks: {
                callback: function(value) {
                  return value.toLocaleString() + '원';
                }
              },
              grid: {
                drawOnChartArea: true
              }
            },
            'y-volume': {
              type: 'linear',
              position: 'right',
              title: {
                display: true,
                text: '거래량'
              },
              ticks: {
                callback: function(value) {
                  if (value >= 1000000) {
                    return (value / 1000000).toFixed(1) + 'M';
                  } else if (value >= 1000) {
                    return (value / 1000).toFixed(0) + 'K';
                  }
                  return value.toLocaleString();
                }
              },
              grid: {
                drawOnChartArea: false
              },
              // 거래량 축의 최대값을 조정하여 차트 높이를 가격의 30% 수준으로
              max: function(context) {
                const maxVolume = Math.max(...volumeData.map(d => d.y));
                return maxVolume * 3.3; // 거래량 차트가 전체의 약 30% 높이를 차지하도록
              }
            }
          },
          plugins: {
            legend: {
              display: false
            },
            tooltip: {
              callbacks: {
                title: function(context) {
                  // raw 데이터의 date 필드 사용
                  const raw = context[0].raw;
                  if (raw.date) {
                    const date = new Date(raw.date);
                    return `${date.getFullYear()}년 ${date.getMonth() + 1}월 ${date.getDate()}일`;
                  }
                  return '';
                },
                label: function(context) {
                  const raw = context.raw;

                  // 캔들스틱 데이터
                  if (raw.o !== undefined) {
                    return [
                      `시가: ${raw.o.toLocaleString()}원`,
                      `고가: ${raw.h.toLocaleString()}원`,
                      `저가: ${raw.l.toLocaleString()}원`,
                      `종가: ${raw.c.toLocaleString()}원`
                    ];
                  }

                  // 거래량 데이터
                  if (raw.y !== undefined) {
                    return `거래량: ${raw.y.toLocaleString()}`;
                  }

                  return '';
                }
              }
            }
          }
        }
      });

      setLoading(false);
    };

    fetchData();

    // Cleanup
    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [stockCode, period, customMode, startDate, endDate]);

  return (
    <div>
      {error && (
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
      )}

      {loading && (
        <div style={{
          padding: '15px',
          margin: '10px 0',
          textAlign: 'center',
          color: '#666'
        }}>
          차트 데이터를 불러오는 중...
        </div>
      )}

      {/* 기간 선택 버튼 */}
      <div style={{
        marginBottom: '15px',
        display: 'flex',
        gap: '10px',
        flexWrap: 'wrap',
        alignItems: 'center'
      }}>
        <button
          onClick={() => { setPeriod('1M'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === '1M' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === '1M' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === '1M' ? 'bold' : 'normal'
          }}
        >
          1개월
        </button>
        <button
          onClick={() => { setPeriod('3M'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === '3M' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === '3M' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === '3M' ? 'bold' : 'normal'
          }}
        >
          3개월
        </button>
        <button
          onClick={() => { setPeriod('6M'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === '6M' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === '6M' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === '6M' ? 'bold' : 'normal'
          }}
        >
          6개월
        </button>
        <button
          onClick={() => { setPeriod('1Y'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === '1Y' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === '1Y' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === '1Y' ? 'bold' : 'normal'
          }}
        >
          1년
        </button>
        <button
          onClick={() => { setPeriod('3Y'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === '3Y' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === '3Y' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === '3Y' ? 'bold' : 'normal'
          }}
        >
          3년
        </button>
        <button
          onClick={() => { setPeriod('ALL'); setCustomMode(false); }}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === 'ALL' ? '#007bff' : '#f0f0f0',
            color: !customMode && period === 'ALL' ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === 'ALL' ? 'bold' : 'normal'
          }}
        >
          전체 (2020~)
        </button>

        <div style={{
          borderLeft: '2px solid #ddd',
          height: '30px',
          margin: '0 5px'
        }}></div>

        <button
          onClick={() => setCustomMode(!customMode)}
          style={{
            padding: '8px 16px',
            backgroundColor: customMode ? '#28a745' : '#6c757d',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: customMode ? 'bold' : 'normal'
          }}
        >
          날짜 지정
        </button>
      </div>

      {/* 사용자 지정 날짜 선택 */}
      {customMode && (
        <div style={{
          marginBottom: '15px',
          padding: '15px',
          backgroundColor: '#f8f9fa',
          borderRadius: '8px',
          border: '1px solid #dee2e6'
        }}>
          <div style={{
            display: 'flex',
            gap: '15px',
            alignItems: 'center',
            flexWrap: 'wrap'
          }}>
            <div>
              <label style={{
                display: 'block',
                marginBottom: '5px',
                fontSize: '14px',
                fontWeight: 'bold',
                color: '#495057'
              }}>
                시작일:
              </label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                min="2020-01-01"
                max={endDate || new Date().toISOString().split('T')[0]}
                style={{
                  padding: '8px',
                  fontSize: '14px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px'
                }}
              />
            </div>
            <div>
              <label style={{
                display: 'block',
                marginBottom: '5px',
                fontSize: '14px',
                fontWeight: 'bold',
                color: '#495057'
              }}>
                종료일:
              </label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                min={startDate || "2020-01-01"}
                max={new Date().toISOString().split('T')[0]}
                style={{
                  padding: '8px',
                  fontSize: '14px',
                  border: '1px solid #ced4da',
                  borderRadius: '4px'
                }}
              />
            </div>
            <button
              onClick={() => {
                setStartDate('');
                setEndDate('');
              }}
              style={{
                padding: '8px 16px',
                backgroundColor: '#dc3545',
                color: 'white',
                border: 'none',
                borderRadius: '4px',
                cursor: 'pointer',
                fontSize: '14px',
                marginTop: '20px'
              }}
            >
              초기화
            </button>
          </div>
          {(startDate || endDate) && (
            <div style={{
              marginTop: '10px',
              fontSize: '13px',
              color: '#6c757d'
            }}>
              {startDate && `시작: ${startDate}`}
              {startDate && endDate && ' ~ '}
              {endDate && `종료: ${endDate}`}
            </div>
          )}
        </div>
      )}

      {/* 차트 영역 */}
      <div style={{ position: 'relative', height: '400px', width: '100%' }}>
        <canvas ref={canvasRef}></canvas>
      </div>
    </div>
  );
}

export default StockChart;

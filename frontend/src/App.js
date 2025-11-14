import React, { useState, useEffect } from 'react';
import StockChart from './StockChart';
import StockSearch from './StockSearch';
import StockComparison from './StockComparison';
import Admin from './Admin';
import { stockAPI } from './services/api';
import './App.css';

function App() {
  const [stocks, setStocks] = useState([]);
  const [selectedStock, setSelectedStock] = useState('005930');
  const [selectedStockName, setSelectedStockName] = useState('삼성전자');
  const [comparisonStocks, setComparisonStocks] = useState([]);
  const [showComparison, setShowComparison] = useState(false);
  const [activeMarket, setActiveMarket] = useState('KOSPI');
  const [currentPage, setCurrentPage] = useState(1);
  const [lastUpdateDate, setLastUpdateDate] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [showAdmin, setShowAdmin] = useState(false);
  const itemsPerPage = 25;

  useEffect(() => {
    // 시장 유형에 따라 종목 가져오기
    const fetchStocks = async () => {
      setLoading(true);
      setError(null);

      const result = await stockAPI.getStocks(activeMarket);

      if (result.error) {
        setError(result.error);
        setStocks([]);
      } else {
        setStocks(result.data);
        // 최신 업데이트 날짜 가져오기 (첫 번째 종목의 last_update_date 사용)
        if (result.data.length > 0 && result.data[0].last_update_date) {
          setLastUpdateDate(result.data[0].last_update_date);
        }
      }

      setLoading(false);
    };

    fetchStocks();
    setCurrentPage(1); // 시장 변경 시 첫 페이지로
  }, [activeMarket]);

  // 시가총액을 시장별로 포맷팅 (KOSPI: 조, KOSDAQ/ETF: 억)
  const formatMarketCap = (marketCap) => {
    if (!marketCap) return '-';

    if (activeMarket === 'KOSPI') {
      // KOSPI는 조 단위
      const trillion = marketCap / 1000000000000;
      return `${trillion.toFixed(1)}조`;
    } else {
      // KOSDAQ, ETF는 억 단위 (천 단위 콤마 추가)
      const hundredMillion = marketCap / 100000000;
      return `${hundredMillion.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}억`;
    }
  };

  // 체크박스 토글
  const toggleComparison = (stock) => {
    const isSelected = comparisonStocks.some(s => s.stock_code === stock.stock_code);

    if (isSelected) {
      setComparisonStocks(comparisonStocks.filter(s => s.stock_code !== stock.stock_code));
    } else {
      setComparisonStocks([...comparisonStocks, stock]);
    }
  };

  // 비교하기 버튼 클릭
  const handleCompare = () => {
    if (comparisonStocks.length >= 2) {
      setShowComparison(true);
    } else {
      alert('비교하려면 최소 2개 이상의 종목을 선택하세요.');
    }
  };

  // 비교 닫기
  const handleCloseComparison = () => {
    setShowComparison(false);
  };

  // 전체 선택/해제
  const handleSelectAll = () => {
    const currentPageStocks = paginatedStocks;
    const allSelected = currentPageStocks.every(stock =>
      comparisonStocks.some(s => s.stock_code === stock.stock_code)
    );

    if (allSelected) {
      // 현재 페이지 전체 해제
      const pageCodes = currentPageStocks.map(s => s.stock_code);
      setComparisonStocks(comparisonStocks.filter(s => !pageCodes.includes(s.stock_code)));
    } else {
      // 현재 페이지 전체 선택
      const newSelections = currentPageStocks.filter(stock =>
        !comparisonStocks.some(s => s.stock_code === stock.stock_code)
      );
      setComparisonStocks([...comparisonStocks, ...newSelections]);
    }
  };

  // 페이지네이션 계산
  const totalPages = Math.ceil(stocks.length / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedStocks = stocks.slice(startIndex, endIndex);

  // 현재 페이지의 전체 선택 여부
  const isAllSelected = paginatedStocks.length > 0 && paginatedStocks.every(stock =>
    comparisonStocks.some(s => s.stock_code === stock.stock_code)
  );

  // 관리자 페이지 표시
  if (showAdmin) {
    return (
      <div className="App">
        <button
          onClick={() => setShowAdmin(false)}
          style={{
            padding: '10px 20px',
            background: '#6c757d',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: 'bold',
            marginBottom: '20px'
          }}
        >
          ← 메인으로 돌아가기
        </button>
        <Admin />
      </div>
    );
  }

  return (
    <div className="App">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h1>📈 주식 분석 시스템</h1>
        <button
          onClick={() => setShowAdmin(true)}
          style={{
            padding: '10px 20px',
            background: '#28a745',
            color: 'white',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: 'bold'
          }}
        >
          ⚙️ 관리자
        </button>
      </div>

      <div className="market-tabs">
        <button
          className={`tab-button ${activeMarket === 'KOSPI' ? 'active' : ''}`}
          onClick={() => setActiveMarket('KOSPI')}
        >
          코스피
        </button>
        <button
          className={`tab-button ${activeMarket === 'KOSDAQ' ? 'active' : ''}`}
          onClick={() => setActiveMarket('KOSDAQ')}
        >
          코스닥
        </button>
        <button
          className={`tab-button ${activeMarket === 'ETF' ? 'active' : ''}`}
          onClick={() => setActiveMarket('ETF')}
        >
          ETF
        </button>
      </div>

      {error && (
        <div className="error-message" style={{
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
        <div className="loading-message" style={{
          padding: '15px',
          margin: '10px 0',
          textAlign: 'center',
          color: '#666'
        }}>
          데이터를 불러오는 중...
        </div>
      )}

      <StockSearch onSelectStock={setSelectedStock} />

      <div className="chart-container">
        <h2>{selectedStockName}</h2>
        <StockChart stockCode={selectedStock} />
      </div>

      <div className="comparison-section">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
          <h2 style={{ margin: 0 }}>{activeMarket} {activeMarket === 'ETF' ? '거래대금' : '시가총액'} 상위 종목 (총 {stocks.length}개)</h2>
          {lastUpdateDate && (
            <div style={{ fontSize: '14px', color: '#666' }}>
              데이터 업데이트: {lastUpdateDate}
            </div>
          )}
        </div>
        <div className="comparison-controls">
          <button
            className="select-all-button"
            onClick={handleSelectAll}
          >
            {isAllSelected ? '현재 페이지 전체 해제' : '현재 페이지 전체 선택'}
          </button>
          {comparisonStocks.length > 0 && (
            <>
              <span className="selected-count">
                선택된 종목: {comparisonStocks.length}개
              </span>
              <button
                className="compare-button"
                onClick={handleCompare}
                disabled={comparisonStocks.length < 2}
              >
                비교하기
              </button>
            </>
          )}
        </div>
      </div>

      {showComparison && (
        <StockComparison
          selectedStocks={comparisonStocks}
          onClose={handleCloseComparison}
        />
      )}

      <table>
        <thead>
          <tr>
            <th>선택</th>
            <th>순위</th>
            <th>종목코드</th>
            <th>종목명</th>
            <th>현재가</th>
            <th>등락율</th>
            <th>거래량</th>
            <th>시가총액</th>
            <th>PER</th>
            <th>PBR</th>
            <th>최신공시</th>
          </tr>
        </thead>
        <tbody>
          {paginatedStocks.map((stock, index) => {
            const isChecked = comparisonStocks.some(s => s.stock_code === stock.stock_code);
            const changeRate = stock.change_rate || 0;
            const changeClass = changeRate > 0 ? 'positive' : changeRate < 0 ? 'negative' : '';
            const actualIndex = startIndex + index + 1;

            return (
              <tr key={stock.stock_code}>
                <td onClick={(e) => e.stopPropagation()}>
                  <input
                    type="checkbox"
                    checked={isChecked}
                    onChange={() => toggleComparison(stock)}
                    className="stock-checkbox"
                  />
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {actualIndex}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {stock.stock_code}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {stock.stock_name}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }} className="number-cell">
                  {stock.current_price ? stock.current_price.toLocaleString() + '원' : '-'}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }} className={`number-cell ${changeClass}`}>
                  {stock.change_rate !== undefined ? (changeRate >= 0 ? '+' : '') + changeRate.toFixed(2) + '%' : '-'}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }} className="number-cell">
                  {stock.volume ? stock.volume.toLocaleString() : '-'}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }} className="number-cell">
                  {formatMarketCap(stock.market_cap)}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {stock.per ? parseFloat(stock.per).toFixed(2) : '-'}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {stock.pbr ? parseFloat(stock.pbr).toFixed(2) : '-'}
                </td>
                <td onClick={() => { setSelectedStock(stock.stock_code); setSelectedStockName(stock.stock_name); }} style={{ cursor: 'pointer' }}>
                  {stock.last_report_date || '-'}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>

      {/* 페이지네이션 */}
      <div className="pagination">
        <button
          className="pagination-button"
          onClick={() => setCurrentPage(1)}
          disabled={currentPage === 1}
        >
          처음
        </button>
        <button
          className="pagination-button"
          onClick={() => setCurrentPage(currentPage - 1)}
          disabled={currentPage === 1}
        >
          이전
        </button>
        <span className="pagination-info">
          {currentPage} / {totalPages} 페이지 ({startIndex + 1}-{Math.min(endIndex, stocks.length)} / {stocks.length})
        </span>
        <button
          className="pagination-button"
          onClick={() => setCurrentPage(currentPage + 1)}
          disabled={currentPage === totalPages}
        >
          다음
        </button>
        <button
          className="pagination-button"
          onClick={() => setCurrentPage(totalPages)}
          disabled={currentPage === totalPages}
        >
          마지막
        </button>
      </div>
    </div>
  );
}

export default App;
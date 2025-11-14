import React, { useState, useEffect } from 'react';
import api from './services/api';
import './Admin.css';

function Admin() {
  const [activeTab, setActiveTab] = useState('health');
  const [healthData, setHealthData] = useState(null);
  const [logFiles, setLogFiles] = useState([]);
  const [selectedLog, setSelectedLog] = useState(null);
  const [logContent, setLogContent] = useState(null);
  const [dbStats, setDbStats] = useState(null);
  const [dataRange, setDataRange] = useState(null);
  const [missingData, setMissingData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(false);

  // 헬스 체크 데이터 로드
  const loadHealthData = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/admin/health');
      setHealthData(response.data);
    } catch (err) {
      setError('헬스 체크 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 로그 파일 목록 로드
  const loadLogFiles = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/admin/logs');
      setLogFiles(response.data);
    } catch (err) {
      setError('로그 파일 목록 조회 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 로그 파일 내용 로드
  const loadLogContent = async (filename, lines = 100) => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get(`/admin/logs/${filename}?lines=${lines}`);
      setLogContent(response.data);
    } catch (err) {
      setError('로그 파일 조회 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // DB 통계 로드
  const loadDbStats = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/admin/stats/db');
      setDbStats(response.data);
    } catch (err) {
      setError('DB 통계 조회 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 데이터 범위 로드
  const loadDataRange = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/admin/stats/data-range');
      setDataRange(response.data);
    } catch (err) {
      setError('데이터 범위 조회 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 데이터 누락 현황 로드
  const loadMissingData = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.get('/admin/stats/missing-data');
      setMissingData(response.data);
    } catch (err) {
      setError('데이터 누락 현황 조회 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 캐시 삭제
  const clearCache = async (key = null) => {
    if (!window.confirm(key ? `캐시 키 "${key}"를 삭제하시겠습니까?` : '전체 캐시를 삭제하시겠습니까?')) {
      return;
    }

    try {
      setLoading(true);
      const url = key ? `/admin/cache?key=${encodeURIComponent(key)}` : '/admin/cache';
      await api.delete(url);
      alert('캐시가 삭제되었습니다.');
      loadHealthData();
    } catch (err) {
      setError('캐시 삭제 실패: ' + (err.response?.data?.error || err.message));
    } finally {
      setLoading(false);
    }
  };

  // 탭 변경 시 데이터 로드
  useEffect(() => {
    switch (activeTab) {
      case 'health':
        loadHealthData();
        break;
      case 'logs':
        loadLogFiles();
        break;
      case 'stats':
        loadDbStats();
        break;
      case 'data-status':
        loadDataRange();
        loadMissingData();
        break;
      default:
        break;
    }
  }, [activeTab]);

  // 자동 새로고침
  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      if (activeTab === 'health') {
        loadHealthData();
      }
    }, 5000); // 5초마다

    return () => clearInterval(interval);
  }, [autoRefresh, activeTab]);

  return (
    <div className="admin-container">
      <h1>관리자 대시보드</h1>

      <div className="admin-tabs">
        <button
          className={activeTab === 'health' ? 'active' : ''}
          onClick={() => setActiveTab('health')}
        >
          서버 상태
        </button>
        <button
          className={activeTab === 'logs' ? 'active' : ''}
          onClick={() => setActiveTab('logs')}
        >
          로그 파일
        </button>
        <button
          className={activeTab === 'stats' ? 'active' : ''}
          onClick={() => setActiveTab('stats')}
        >
          DB 통계
        </button>
        <button
          className={activeTab === 'data-status' ? 'active' : ''}
          onClick={() => setActiveTab('data-status')}
        >
          데이터 현황
        </button>
      </div>

      {error && <div className="error-message">{error}</div>}
      {loading && <div className="loading">로딩 중...</div>}

      {/* 서버 상태 탭 */}
      {activeTab === 'health' && healthData && (
        <div className="health-section">
          <div className="section-header">
            <h2>서버 헬스 체크</h2>
            <div className="controls">
              <label>
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={(e) => setAutoRefresh(e.target.checked)}
                />
                자동 새로고침 (5초)
              </label>
              <button onClick={loadHealthData}>새로고침</button>
            </div>
          </div>

          <div className={`status-badge ${healthData.status}`}>
            {healthData.status === 'healthy' ? '✅ 정상' : '❌ 비정상'}
          </div>

          <div className="info-grid">
            <div className="info-card">
              <h3>서버 정보</h3>
              <p><strong>환경:</strong> {healthData.environment}</p>
              <p><strong>업타임:</strong> {healthData.uptime.formatted}</p>
              <p><strong>Node 버전:</strong> {healthData.nodeVersion}</p>
              <p><strong>플랫폼:</strong> {healthData.platform}</p>
              <p><strong>타임스탬프:</strong> {new Date(healthData.timestamp).toLocaleString()}</p>
            </div>

            <div className="info-card">
              <h3>데이터베이스</h3>
              <p><strong>상태:</strong> <span className={healthData.database.status}>{healthData.database.status}</span></p>
              <p><strong>호스트:</strong> {healthData.database.host}:{healthData.database.port}</p>
              <p><strong>데이터베이스:</strong> {healthData.database.database}</p>
              {healthData.database.error && <p className="error"><strong>에러:</strong> {healthData.database.error}</p>}
            </div>

            <div className="info-card">
              <h3>메모리 사용량</h3>
              <p><strong>RSS:</strong> {healthData.memory.rss}</p>
              <p><strong>Heap Total:</strong> {healthData.memory.heapTotal}</p>
              <p><strong>Heap Used:</strong> {healthData.memory.heapUsed}</p>
              <p><strong>External:</strong> {healthData.memory.external}</p>
            </div>

            <div className="info-card">
              <h3>캐시 현황</h3>
              <p><strong>캐시 크기:</strong> {healthData.cache.size}개</p>
              <div className="cache-keys">
                {healthData.cache.keys.length > 0 ? (
                  <ul>
                    {healthData.cache.keys.map((key, index) => (
                      <li key={index}>
                        {key}
                        <button
                          onClick={() => clearCache(key)}
                          className="delete-btn"
                        >
                          삭제
                        </button>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p>캐시된 데이터가 없습니다.</p>
                )}
              </div>
              {healthData.cache.size > 0 && (
                <button onClick={() => clearCache()} className="clear-all-btn">
                  전체 캐시 삭제
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* 로그 파일 탭 */}
      {activeTab === 'logs' && (
        <div className="logs-section">
          <div className="section-header">
            <h2>로그 파일</h2>
            <button onClick={loadLogFiles}>새로고침</button>
          </div>

          <div className="logs-layout">
            <div className="log-files-list">
              <h3>파일 목록</h3>
              {logFiles.map((file, index) => (
                <div
                  key={index}
                  className={`log-file-item ${selectedLog === file.name ? 'selected' : ''}`}
                  onClick={() => {
                    setSelectedLog(file.name);
                    loadLogContent(file.name);
                  }}
                >
                  <div className="file-name">{file.name}</div>
                  <div className="file-info">
                    <span>{file.sizeFormatted}</span>
                    <span>{new Date(file.modified).toLocaleString()}</span>
                  </div>
                </div>
              ))}
            </div>

            <div className="log-content-area">
              {logContent ? (
                <>
                  <div className="log-header">
                    <h3>{logContent.filename}</h3>
                    <div className="log-controls">
                      <select
                        onChange={(e) => loadLogContent(selectedLog, e.target.value)}
                        defaultValue="100"
                      >
                        <option value="50">최근 50줄</option>
                        <option value="100">최근 100줄</option>
                        <option value="500">최근 500줄</option>
                        <option value="1000">최근 1000줄</option>
                      </select>
                      <button onClick={() => loadLogContent(selectedLog, logContent.displayedLines)}>
                        새로고침
                      </button>
                    </div>
                    <p>전체 {logContent.totalLines}줄 중 {logContent.displayedLines}줄 표시</p>
                  </div>
                  <pre className="log-content">
                    {logContent.lines.join('\n')}
                  </pre>
                </>
              ) : (
                <div className="no-log-selected">
                  로그 파일을 선택하세요
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* DB 통계 탭 */}
      {activeTab === 'stats' && dbStats && (
        <div className="stats-section">
          <div className="section-header">
            <h2>데이터베이스 통계</h2>
            <button onClick={loadDbStats}>새로고침</button>
          </div>

          <div className="info-grid">
            <div className="info-card">
              <h3>종목 정보</h3>
              <p><strong>전체 종목 수:</strong> {dbStats.stocks.total.toLocaleString()}개</p>
              <h4>시장별 종목 수:</h4>
              <ul>
                {dbStats.stocks.byMarket.map((market, index) => (
                  <li key={index}>
                    <strong>{market.market_type}:</strong> {parseInt(market.count).toLocaleString()}개
                  </li>
                ))}
              </ul>
            </div>

            <div className="info-card">
              <h3>일별 시세 데이터</h3>
              <p><strong>전체 레코드 수:</strong> {dbStats.dailyPrices.total.toLocaleString()}개</p>
              <p><strong>최신 데이터:</strong> {dbStats.dailyPrices.latestDate}</p>
              <p><strong>가장 오래된 데이터:</strong> {dbStats.dailyPrices.oldestDate}</p>
            </div>

            <div className="info-card">
              <h3>테이블 크기</h3>
              <p><strong>stocks 테이블:</strong> {dbStats.tableSize.stocks_size}</p>
              <p><strong>daily_prices 테이블:</strong> {dbStats.tableSize.daily_prices_size}</p>
            </div>
          </div>
        </div>
      )}

      {/* 데이터 현황 탭 */}
      {activeTab === 'data-status' && (
        <div className="data-status-section">
          <div className="section-header">
            <h2>데이터 현황</h2>
            <button onClick={() => { loadDataRange(); loadMissingData(); }}>새로고침</button>
          </div>

          {/* 데이터 범위 */}
          {dataRange && (
            <div style={{ marginBottom: '40px' }}>
              <h3 style={{ marginBottom: '15px', color: '#333' }}>📅 데이터 범위 (시작일 ~ 최근 적재일)</h3>
              <div className="info-grid">
                {dataRange.markets && dataRange.markets.map((market, index) => (
                  <div key={index} className="info-card">
                    <h3>{market.market_type}</h3>
                    <p><strong>종목 수:</strong> {parseInt(market.stock_count).toLocaleString()}개</p>
                    <p><strong>시작일:</strong> {market.start_date || '-'}</p>
                    <p><strong>최근 적재일:</strong> {market.latest_date || '-'}</p>
                    <p><strong>총 레코드:</strong> {parseInt(market.total_records).toLocaleString()}건</p>
                  </div>
                ))}
                {dataRange.etf && (
                  <div className="info-card">
                    <h3>ETF</h3>
                    <p><strong>종목 수:</strong> {parseInt(dataRange.etf.stock_count).toLocaleString()}개</p>
                    <p><strong>시작일:</strong> {dataRange.etf.start_date || '-'}</p>
                    <p><strong>최근 적재일:</strong> {dataRange.etf.latest_date || '-'}</p>
                    <p><strong>총 레코드:</strong> {parseInt(dataRange.etf.total_records).toLocaleString()}건</p>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* 데이터 누락 현황 */}
          {missingData && (
            <div>
              <h3 style={{ marginBottom: '15px', color: '#333' }}>⚠️ 데이터 누락 현황 (컬럼별)</h3>
              <div className="info-grid">
                {missingData.markets && missingData.markets.map((market, index) => (
                  <div key={index} className="info-card">
                    <h3>{market.market_type}</h3>
                    <p><strong>총 레코드:</strong> {parseInt(market.total_records).toLocaleString()}건</p>
                    <div style={{ marginTop: '15px', fontSize: '14px' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                        <thead>
                          <tr style={{ borderBottom: '2px solid #e0e0e0' }}>
                            <th style={{ textAlign: 'left', padding: '8px' }}>컬럼</th>
                            <th style={{ textAlign: 'right', padding: '8px' }}>누락</th>
                            <th style={{ textAlign: 'right', padding: '8px' }}>비율</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>시가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_open_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_open_price) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>고가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_high_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_high_price) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>저가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_low_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_low_price) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>종가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_close_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_close_price) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>거래량</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_volume).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_volume) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>전일대비</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_vs).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_vs) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>등락율</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_change_rate).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_change_rate) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>거래대금</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(market.missing_trading_value).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(market.missing_trading_value) / parseInt(market.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                ))}
                {missingData.etf && (
                  <div className="info-card">
                    <h3>ETF</h3>
                    <p><strong>총 레코드:</strong> {parseInt(missingData.etf.total_records).toLocaleString()}건</p>
                    <div style={{ marginTop: '15px', fontSize: '14px' }}>
                      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                        <thead>
                          <tr style={{ borderBottom: '2px solid #e0e0e0' }}>
                            <th style={{ textAlign: 'left', padding: '8px' }}>컬럼</th>
                            <th style={{ textAlign: 'right', padding: '8px' }}>누락</th>
                            <th style={{ textAlign: 'right', padding: '8px' }}>비율</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>시가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_open_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_open_price) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>고가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_high_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_high_price) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>저가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_low_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_low_price) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>종가</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_close_price).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_close_price) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>거래량</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_volume).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_volume) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>전일대비</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_vs).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_vs) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>등락율</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_change_rate).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_change_rate) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                          <tr>
                            <td style={{ padding: '5px 8px' }}>거래대금</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>{parseInt(missingData.etf.missing_trading_value).toLocaleString()}</td>
                            <td style={{ textAlign: 'right', padding: '5px 8px' }}>
                              {((parseInt(missingData.etf.missing_trading_value) / parseInt(missingData.etf.total_records)) * 100).toFixed(2)}%
                            </td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default Admin;

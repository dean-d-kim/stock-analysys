import React, { useState } from 'react';
import { stockAPI } from './services/api';

function StockSearch({ onSelectStock }) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleSearch = async (e) => {
    const value = e.target.value;
    setQuery(value);

    if (value.length > 0) {
      setLoading(true);
      setError(null);

      const result = await stockAPI.searchStocks(value);

      if (result.error) {
        setError(result.error);
        setResults([]);
      } else {
        setResults(result.data);
      }

      setLoading(false);
    } else {
      setResults([]);
      setError(null);
    }
  };

  return (
    <div className="search-container">
      <input
        type="text"
        placeholder="종목명 또는 코드 검색..."
        value={query}
        onChange={handleSearch}
        className="search-input"
      />
      {loading && (
        <div className="search-results" style={{ padding: '10px', textAlign: 'center', color: '#666' }}>
          검색 중...
        </div>
      )}
      {error && (
        <div className="search-results" style={{ padding: '10px', color: '#c33' }}>
          {error}
        </div>
      )}
      {!loading && !error && results.length > 0 && (
        <div className="search-results">
          {results.map(stock => (
            <div
              key={stock.stock_code}
              className="search-item"
              onClick={() => {
                onSelectStock(stock.stock_code);
                setQuery('');
                setResults([]);
              }}
            >
              <strong>{stock.stock_name}</strong> ({stock.stock_code})
              <span className="badge">{stock.market_type}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default StockSearch;
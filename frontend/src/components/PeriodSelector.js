import React from 'react';

const PERIODS = [
  { value: '1M', label: '1개월' },
  { value: '3M', label: '3개월' },
  { value: '6M', label: '6개월' },
  { value: '1Y', label: '1년' },
  { value: '3Y', label: '3년' },
  { value: 'ALL', label: '전체 (2020~)' }
];

function PeriodSelector({ period, customMode, onPeriodChange, onCustomModeToggle }) {
  return (
    <div style={{
      marginBottom: '15px',
      display: 'flex',
      gap: '10px',
      flexWrap: 'wrap',
      alignItems: 'center'
    }}>
      {PERIODS.map(({ value, label }) => (
        <button
          key={value}
          onClick={() => onPeriodChange(value)}
          style={{
            padding: '8px 16px',
            backgroundColor: !customMode && period === value ? '#007bff' : '#f0f0f0',
            color: !customMode && period === value ? 'white' : 'black',
            border: 'none',
            borderRadius: '4px',
            cursor: 'pointer',
            fontSize: '14px',
            fontWeight: !customMode && period === value ? 'bold' : 'normal'
          }}
        >
          {label}
        </button>
      ))}

      <div style={{
        borderLeft: '2px solid #ddd',
        height: '30px',
        margin: '0 5px'
      }}></div>

      <button
        onClick={onCustomModeToggle}
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
  );
}

export default PeriodSelector;

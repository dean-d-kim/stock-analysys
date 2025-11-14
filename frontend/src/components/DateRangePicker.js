import React from 'react';

function DateRangePicker({ startDate, endDate, onStartDateChange, onEndDateChange, onReset }) {
  const today = new Date().toISOString().split('T')[0];

  return (
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
            onChange={(e) => onStartDateChange(e.target.value)}
            min="2020-01-01"
            max={endDate || today}
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
            onChange={(e) => onEndDateChange(e.target.value)}
            min={startDate || "2020-01-01"}
            max={today}
            style={{
              padding: '8px',
              fontSize: '14px',
              border: '1px solid #ced4da',
              borderRadius: '4px'
            }}
          />
        </div>
        <button
          onClick={onReset}
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
  );
}

export default DateRangePicker;

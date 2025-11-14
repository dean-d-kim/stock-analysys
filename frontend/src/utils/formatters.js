// 시가총액 포맷팅 (시장별)
export const formatMarketCap = (marketCap, marketType) => {
  if (!marketCap) return '-';

  if (marketType === 'KOSPI') {
    // KOSPI는 조 단위
    const trillion = marketCap / 1000000000000;
    return `${trillion.toFixed(1)}조`;
  } else {
    // KOSDAQ, ETF는 억 단위 (천 단위 콤마 추가)
    const hundredMillion = marketCap / 100000000;
    return `${hundredMillion.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ',')}억`;
  }
};

// 숫자에 콤마 추가
export const formatNumber = (number) => {
  if (number === null || number === undefined) return '-';
  return number.toLocaleString();
};

// 등락율 포맷팅
export const formatChangeRate = (rate) => {
  if (rate === null || rate === undefined) return '-';
  const sign = rate >= 0 ? '+' : '';
  return `${sign}${rate.toFixed(2)}%`;
};

// 날짜 포맷팅 (MM/DD)
export const formatDate = (dateString) => {
  const date = new Date(dateString);
  return `${date.getMonth() + 1}/${date.getDate()}`;
};

// 가격 포맷팅 (원)
export const formatPrice = (price) => {
  if (!price) return '-';
  return `${formatNumber(price)}원`;
};

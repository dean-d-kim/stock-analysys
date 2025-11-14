import axios from 'axios';

const API_BASE_URL = '/api';

// API 클라이언트 설정
const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json'
  }
});

// 에러 처리 헬퍼
const handleAPIError = (error) => {
  if (error.response) {
    // 서버 응답 에러 (4xx, 5xx)
    const { status, data } = error.response;
    const message = data?.error || data?.message || '서버 오류가 발생했습니다.';

    switch (status) {
      case 400:
        return { error: '잘못된 요청입니다.', details: data?.details, originalError: error };
      case 404:
        return { error: '데이터를 찾을 수 없습니다.', details: message, originalError: error };
      case 500:
        return { error: '서버 오류가 발생했습니다.', details: message, originalError: error };
      default:
        return { error: message, originalError: error };
    }
  } else if (error.request) {
    // 요청은 보냈지만 응답을 받지 못함 (네트워크 오류)
    return { error: '서버와 연결할 수 없습니다. 네트워크를 확인해주세요.', originalError: error };
  } else {
    // 요청 설정 중 오류 발생
    return { error: '요청 중 오류가 발생했습니다.', details: error.message, originalError: error };
  }
};

// 응답 인터셉터 - 에러 처리
apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API 에러:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

// 주식 API
export const stockAPI = {
  // 종목 목록 조회
  getStocks: async (marketType) => {
    try {
      const params = marketType ? { market_type: marketType } : {};
      const response = await apiClient.get('/stocks', { params });
      return { data: response.data, error: null };
    } catch (error) {
      return { data: null, ...handleAPIError(error) };
    }
  },

  // 종목 검색
  searchStocks: async (query) => {
    try {
      const response = await apiClient.get('/stocks/search', {
        params: { q: query }
      });
      return { data: response.data, error: null };
    } catch (error) {
      return { data: null, ...handleAPIError(error) };
    }
  },

  // 종목 상세 조회
  getStockDetail: async (stockCode) => {
    try {
      const response = await apiClient.get(`/stocks/${stockCode}`);
      return { data: response.data, error: null };
    } catch (error) {
      return { data: null, ...handleAPIError(error) };
    }
  },

  // 일별 시세 조회
  getDailyPrices: async (stockCode, limit = 100) => {
    try {
      const response = await apiClient.get(`/stocks/${stockCode}/daily`, {
        params: { limit }
      });
      return { data: response.data, error: null };
    } catch (error) {
      return { data: null, ...handleAPIError(error) };
    }
  }
};

export default apiClient;

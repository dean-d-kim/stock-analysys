const swaggerJsdoc = require('swagger-jsdoc');
const config = require('./config');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Stock Analysis API',
      version: '1.0.0',
      description: '한국 주식 시장 데이터 분석 API',
      contact: {
        name: 'API Support',
      },
    },
    servers: [
      {
        url: `http://localhost:${config.server.port}`,
        description: '개발 서버',
      },
    ],
    tags: [
      {
        name: 'Stocks',
        description: '주식 종목 관련 API',
      },
    ],
    components: {
      schemas: {
        Stock: {
          type: 'object',
          properties: {
            stock_code: {
              type: 'string',
              description: '종목 코드',
              example: '005930',
            },
            stock_name: {
              type: 'string',
              description: '종목명',
              example: '삼성전자',
            },
            market_type: {
              type: 'string',
              enum: ['KOSPI', 'KOSDAQ', 'ETF', 'KONEX'],
              description: '시장 구분',
            },
            market_cap: {
              type: 'number',
              description: '시가총액',
              example: 500000000000,
            },
            listed_shares: {
              type: 'number',
              description: '상장 주식 수',
              example: 5000000000,
            },
            current_price: {
              type: 'number',
              description: '현재가',
              example: 70000,
            },
            volume: {
              type: 'integer',
              description: '거래량',
              example: 15000000,
            },
            change_rate: {
              type: 'number',
              description: '등락률 (%)',
              example: 2.5,
            },
            last_update_date: {
              type: 'string',
              format: 'date',
              description: '마지막 업데이트 날짜',
              example: '2025-11-12',
            },
          },
        },
        DailyPrice: {
          type: 'object',
          properties: {
            id: {
              type: 'integer',
              description: '일별 시세 ID',
            },
            stock_code: {
              type: 'string',
              description: '종목 코드',
              example: '005930',
            },
            trade_date: {
              type: 'string',
              format: 'date',
              description: '거래일',
              example: '2025-11-12',
            },
            open_price: {
              type: 'number',
              description: '시가',
              example: 69500,
            },
            high_price: {
              type: 'number',
              description: '고가',
              example: 70500,
            },
            low_price: {
              type: 'number',
              description: '저가',
              example: 69000,
            },
            close_price: {
              type: 'number',
              description: '종가',
              example: 70000,
            },
            volume: {
              type: 'integer',
              description: '거래량',
              example: 15000000,
            },
            trading_value: {
              type: 'number',
              description: '거래대금',
              example: 1050000000000,
            },
            change_rate: {
              type: 'number',
              description: '등락률 (%)',
              example: 0.72,
            },
            vs: {
              type: 'number',
              description: '전일 대비',
              example: 500,
            },
          },
        },
        Error: {
          type: 'object',
          properties: {
            error: {
              type: 'string',
              description: '에러 메시지',
            },
            details: {
              type: 'array',
              items: {
                type: 'object',
              },
              description: '상세 에러 정보',
            },
          },
        },
      },
      parameters: {
        stockCode: {
          name: 'code',
          in: 'path',
          required: true,
          description: '종목 코드',
          schema: {
            type: 'string',
            pattern: '^[0-9A-Z]{6,10}$',
          },
          example: '005930',
        },
        marketType: {
          name: 'market_type',
          in: 'query',
          required: false,
          description: '시장 구분 필터',
          schema: {
            type: 'string',
            enum: ['KOSPI', 'KOSDAQ', 'ETF'],
          },
        },
        searchQuery: {
          name: 'q',
          in: 'query',
          required: true,
          description: '검색 쿼리 (종목명 또는 종목코드)',
          schema: {
            type: 'string',
            minLength: 1,
            maxLength: 50,
          },
        },
        limit: {
          name: 'limit',
          in: 'query',
          required: false,
          description: '조회할 데이터 개수',
          schema: {
            type: 'integer',
            minimum: 1,
            maximum: 10000,
            default: 365,
          },
        },
      },
      responses: {
        BadRequest: {
          description: '잘못된 요청',
          content: {
            'application/json': {
              schema: {
                $ref: '#/components/schemas/Error',
              },
            },
          },
        },
        NotFound: {
          description: '리소스를 찾을 수 없음',
          content: {
            'application/json': {
              schema: {
                $ref: '#/components/schemas/Error',
              },
            },
          },
        },
        ServerError: {
          description: '서버 오류',
          content: {
            'application/json': {
              schema: {
                $ref: '#/components/schemas/Error',
              },
            },
          },
        },
      },
    },
  },
  apis: ['./routes/*.js'],
};

const specs = swaggerJsdoc(options);

module.exports = specs;

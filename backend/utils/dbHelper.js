const pool = require('../db');
const logger = require('./logger');

/**
 * 데이터베이스 쿼리 헬퍼 함수
 * 에러 처리 및 로깅을 자동화
 */

/**
 * 단일 쿼리 실행
 * @param {string} query - SQL 쿼리
 * @param {Array} params - 쿼리 파라미터
 * @param {string} context - 로깅용 컨텍스트 정보
 * @returns {Promise<Object>} 쿼리 결과
 */
async function executeQuery(query, params = [], context = '') {
  const startTime = Date.now();

  try {
    const result = await pool.query(query, params);
    const duration = Date.now() - startTime;

    logger.debug('쿼리 실행 완료', {
      context,
      duration: `${duration}ms`,
      rowCount: result.rowCount
    });

    return result;
  } catch (error) {
    const duration = Date.now() - startTime;

    logger.error('쿼리 실행 실패', {
      context,
      duration: `${duration}ms`,
      error: error.message,
      query: query.substring(0, 200), // 처음 200자만 로깅
      params,
      stack: error.stack
    });

    throw error;
  }
}

/**
 * 단일 행 조회
 * @param {string} query - SQL 쿼리
 * @param {Array} params - 쿼리 파라미터
 * @param {string} context - 로깅용 컨텍스트 정보
 * @returns {Promise<Object|null>} 단일 행 또는 null
 */
async function findOne(query, params = [], context = '') {
  const result = await executeQuery(query, params, context);
  return result.rows[0] || null;
}

/**
 * 다중 행 조회
 * @param {string} query - SQL 쿼리
 * @param {Array} params - 쿼리 파라미터
 * @param {string} context - 로깅용 컨텍스트 정보
 * @returns {Promise<Array>} 행 배열
 */
async function findMany(query, params = [], context = '') {
  const result = await executeQuery(query, params, context);
  return result.rows;
}

/**
 * 행 존재 여부 확인
 * @param {string} table - 테이블명
 * @param {string} condition - WHERE 조건
 * @param {Array} params - 쿼리 파라미터
 * @returns {Promise<boolean>} 존재 여부
 */
async function exists(table, condition, params = []) {
  const query = `SELECT EXISTS(SELECT 1 FROM ${table} WHERE ${condition})`;
  const result = await executeQuery(query, params, `exists: ${table}`);
  return result.rows[0].exists;
}

/**
 * 행 개수 조회
 * @param {string} table - 테이블명
 * @param {string} condition - WHERE 조건 (선택적)
 * @param {Array} params - 쿼리 파라미터
 * @returns {Promise<number>} 행 개수
 */
async function count(table, condition = '', params = []) {
  const whereClause = condition ? `WHERE ${condition}` : '';
  const query = `SELECT COUNT(*) FROM ${table} ${whereClause}`;
  const result = await executeQuery(query, params, `count: ${table}`);
  return parseInt(result.rows[0].count);
}

/**
 * 트랜잭션 실행
 * @param {Function} callback - 트랜잭션 콜백 함수
 * @returns {Promise<any>} 콜백 함수의 반환값
 */
async function transaction(callback) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    logger.debug('트랜잭션 시작');

    const result = await callback(client);

    await client.query('COMMIT');
    logger.debug('트랜잭션 커밋');

    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    logger.error('트랜잭션 롤백', {
      error: error.message,
      stack: error.stack
    });
    throw error;
  } finally {
    client.release();
  }
}

/**
 * 페이지네이션 조회
 * @param {string} query - 기본 SELECT 쿼리 (ORDER BY 포함)
 * @param {Array} params - 쿼리 파라미터
 * @param {number} page - 페이지 번호 (1부터 시작)
 * @param {number} pageSize - 페이지당 항목 수
 * @param {string} context - 로깅용 컨텍스트 정보
 * @returns {Promise<Object>} { data, page, pageSize, totalCount, totalPages }
 */
async function paginate(query, params = [], page = 1, pageSize = 20, context = '') {
  const offset = (page - 1) * pageSize;

  // 총 개수 조회 (원본 쿼리에서 COUNT 추출)
  const countQuery = query.replace(/SELECT .+ FROM/, 'SELECT COUNT(*) FROM').split('ORDER BY')[0];
  const countResult = await executeQuery(countQuery, params, `${context} - count`);
  const totalCount = parseInt(countResult.rows[0].count);

  // 페이지네이션된 데이터 조회
  const paginatedQuery = `${query} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
  const dataResult = await executeQuery(
    paginatedQuery,
    [...params, pageSize, offset],
    `${context} - data`
  );

  return {
    data: dataResult.rows,
    page,
    pageSize,
    totalCount,
    totalPages: Math.ceil(totalCount / pageSize)
  };
}

/**
 * 배치 삽입 (Bulk Insert)
 * @param {string} table - 테이블명
 * @param {Array} columns - 컬럼명 배열
 * @param {Array} values - 값 배열의 배열
 * @returns {Promise<Object>} 쿼리 결과
 */
async function bulkInsert(table, columns, values) {
  if (values.length === 0) {
    return { rowCount: 0 };
  }

  const placeholders = values
    .map((_, i) => {
      const start = i * columns.length + 1;
      const params = columns.map((_, j) => `$${start + j}`).join(', ');
      return `(${params})`;
    })
    .join(', ');

  const query = `
    INSERT INTO ${table} (${columns.join(', ')})
    VALUES ${placeholders}
    ON CONFLICT DO NOTHING
  `;

  const flatValues = values.flat();

  return executeQuery(query, flatValues, `bulkInsert: ${table}`);
}

module.exports = {
  executeQuery,
  findOne,
  findMany,
  exists,
  count,
  transaction,
  paginate,
  bulkInsert
};

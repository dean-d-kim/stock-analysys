const { query, param, validationResult } = require('express-validator');

// 검증 에러 처리 미들웨어
const handleValidationErrors = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      error: 'Validation failed',
      details: errors.array().map(err => ({
        field: err.param,
        message: err.msg,
        value: err.value
      }))
    });
  }
  next();
};

// 주식 목록 조회 검증
const validateStockList = [
  query('market_type')
    .optional()
    .isIn(['KOSPI', 'KOSDAQ', 'ETF'])
    .withMessage('market_type must be one of: KOSPI, KOSDAQ, ETF'),
  handleValidationErrors
];

// 주식 검색 검증
const validateStockSearch = [
  query('q')
    .notEmpty()
    .withMessage('Search query is required')
    .isLength({ min: 1, max: 50 })
    .withMessage('Search query must be between 1 and 50 characters')
    .trim(),
  handleValidationErrors
];

// 주식 코드 검증
const validateStockCode = [
  param('code')
    .notEmpty()
    .withMessage('Stock code is required')
    .matches(/^[0-9A-Z]{6,10}$/)
    .withMessage('Invalid stock code format'),
  handleValidationErrors
];

// 일별 시세 조회 검증
const validateDailyPrices = [
  param('code')
    .notEmpty()
    .withMessage('Stock code is required')
    .matches(/^[0-9A-Z]{6,10}$/)
    .withMessage('Invalid stock code format'),
  query('limit')
    .optional()
    .isInt({ min: 1, max: 10000 })
    .withMessage('Limit must be between 1 and 10000')
    .toInt(),
  handleValidationErrors
];

module.exports = {
  validateStockList,
  validateStockSearch,
  validateStockCode,
  validateDailyPrices,
  handleValidationErrors
};

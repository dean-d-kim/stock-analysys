const express = require('express');
const cors = require('cors');
const compression = require('compression');
const swaggerUi = require('swagger-ui-express');
const config = require('./config/config');
const swaggerSpecs = require('./config/swagger');
const logger = require('./utils/logger');
const requestLogger = require('./middleware/requestLogger');
const { errorHandler, notFoundHandler } = require('./middleware/errorHandler');
const stocksRouter = require('./routes/stocks');
const adminRouter = require('./routes/admin');

const app = express();

// 미들웨어
app.use(cors());
app.use(compression()); // gzip 압축 활성화
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use(requestLogger); // HTTP 요청 로깅

// API 문서
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerSpecs));

// 라우터
app.use('/api/stocks', stocksRouter);
app.use('/api/admin', adminRouter);

// 404 핸들러
app.use(notFoundHandler);

// 에러 핸들러
app.use(errorHandler);

// 서버 시작
app.listen(config.server.port, () => {
  logger.info(`서버 실행: http://localhost:${config.server.port}`);
  logger.info(`환경: ${config.server.env}`);
});

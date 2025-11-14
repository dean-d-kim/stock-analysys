const path = require('path');

// 프로젝트 루트의 .env 파일 로드
require('dotenv').config({
  path: path.join(__dirname, '../../.env')
});

module.exports = {
  server: {
    port: process.env.PORT || 3000,
    env: process.env.NODE_ENV || 'development'
  },
  database: {
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    database: process.env.DB_NAME,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD
  }
};

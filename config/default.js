/**
 * The default configuration file.
 */

module.exports = {
  LOG_LEVEL: process.env.LOG_LEVEL || 'debug',

  KAFKA_URL: process.env.KAFKA_URL || 'localhost:9092',
  // below are used for secure Kafka connection, they are optional
  // for the local Kafka, they are not needed
  KAFKA_CLIENT_CERT: process.env.KAFKA_CLIENT_CERT,
  KAFKA_CLIENT_CERT_KEY: process.env.KAFKA_CLIENT_CERT_KEY,

  // Kafka group id
  KAFKA_GROUP_ID: process.env.KAFKA_GROUP_ID || 'legacy-payment-processor',
  KAFKA_ERROR_TOPIC: process.env.KAFKA_ERROR_TOPIC || 'common.error.reporting',

  // Topics to listen
  UPDATE_CHALLENGE_TOPIC: process.env.UPDATE_CHALLENGE_TOPIC || 'challenge.notification.update',

  // Auth0 parameters
  AUTH0_URL: process.env.AUTH0_URL || 'https://topcoder-dev.auth0.com/oauth/token',
  AUTH0_AUDIENCE: process.env.AUTH0_AUDIENCE || 'https://m2m.topcoder-dev.com/',
  TOKEN_CACHE_TIME: process.env.TOKEN_CACHE_TIME || 90,
  AUTH0_CLIENT_ID: process.env.AUTH0_CLIENT_ID || '',
  AUTH0_CLIENT_SECRET: process.env.AUTH0_CLIENT_SECRET || '',
  AUTH0_PROXY_SERVER_URL: process.env.AUTH0_PROXY_SERVER_URL,

  INFORMIX: {
    SERVER: process.env.INFORMIX_SERVER || 'informixoltp_tcp', // informix server
    DATABASE: process.env.INFORMIX_DATABASE || 'informixoltp', // informix database
    HOST: process.env.INFORMIX_HOST || 'localhost', // host
    PROTOCOL: process.env.INFORMIX_PROTOCOL || 'onsoctcp',
    PORT: process.env.INFORMIX_PORT || '2021', // port
    DB_LOCALE: process.env.INFORMIX_DB_LOCALE || 'en_US.57372',
    USER: process.env.INFORMIX_USER || 'informix', // user
    PASSWORD: process.env.INFORMIX_PASSWORD || '1nf0rm1x', // password
    POOL_MAX_SIZE: parseInt(process.env.MAXPOOL, 10) || 60
  },
  // Topcoder APIs
  TC_API: process.env.TC_API || 'https://api.topcoder-dev.com/v5',

  PAYMENT_STATUS_ID: process.env.PAYMENT_STATUS_ID || 56,
  MODIFICATION_RATIONALE_ID: process.env.MODIFICATION_RATIONALE_ID || 1,
  WINNER_PAYMENT_TYPE_ID: process.env.WINNER_PAYMENT_TYPE_ID || 72,
  COPILOT_PAYMENT_TYPE_ID: process.env.COPILOT_PAYMENT_TYPE_ID || 74,

  V5_PAYMENT_DETAIL_STATUS_ID: process.env.V5_PAYMENT_DETAIL_STATUS_ID || 500,

  PAYMENT_METHOD_ID: process.env.PAYMENT_METHOD_ID || 1,
  CHARITY_IND: process.env.CHARITY_IND || 0,
  INSTALLMENT_NUMBER: process.env.INSTALLMENT_NUMBER || 1,
  COPILOT_ROLE_ID: process.env.COPILOT_ROLE_ID || 'cfe12b3f-2a24-4639-9d8b-ec86726f76bd'
}

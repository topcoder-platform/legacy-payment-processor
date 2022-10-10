/**
 * Contains generic helper methods
 */

const _ = require('lodash')
const config = require('config')
const { Pool } = require('ifxnjs')
const request = require('superagent')
const m2mAuth = require('tc-core-library-js').auth.m2m
const m2m = m2mAuth(_.pick(config, ['AUTH0_URL', 'AUTH0_AUDIENCE', 'TOKEN_CACHE_TIME', 'AUTH0_PROXY_SERVER_URL']))

// Db connection pool
const pool = Promise.promisifyAll(new Pool())
pool.setMaxPoolSize(config.get('INFORMIX.POOL_MAX_SIZE'))

/**
 * Get Informix connection using the configured parameters
 * @return {Object} Informix connection
 */
async function getInformixConnection () {
  // construct the connection string from the configuration parameters.
  const connectionString = 'SERVER=' + config.get('INFORMIX.SERVER') +
                           ';DATABASE=' + config.get('INFORMIX.DATABASE') +
                           ';HOST=' + config.get('INFORMIX.HOST') +
                           ';Protocol=' + config.get('INFORMIX.PROTOCOL') +
                           ';SERVICE=' + config.get('INFORMIX.PORT') +
                           ';DB_LOCALE=' + config.get('INFORMIX.DB_LOCALE') +
                           ';UID=' + config.get('INFORMIX.USER') +
                           ';PWD=' + config.get('INFORMIX.PASSWORD')
  const conn = await pool.openAsync(connectionString)
  return Promise.promisifyAll(conn)
}

/**
 * Get Kafka options
 * @return {Object} the Kafka options
 */
function getKafkaOptions () {
  const options = { connectionString: config.KAFKA_URL, groupId: config.KAFKA_GROUP_ID }
  if (config.KAFKA_CLIENT_CERT && config.KAFKA_CLIENT_CERT_KEY) {
    options.ssl = { cert: config.KAFKA_CLIENT_CERT, key: config.KAFKA_CLIENT_CERT_KEY }
  }
  return options
}

/**
 * Submit a request to zendesk
 * @param {Object} request the request
 */
 async function submitZendeskRequest (requestBody) {
  try {
    return request
      .post(`${config.ZENDESK_API_URL}/api/v2/requests`)
      .send({
        request: {
          ...requestBody
        }
      })
      .auth(`${config.ZENDESK_REQUESTER_EMAIL}/token`, config.ZENDESK_API_TOKEN)
  } catch (e) {
    logger.debug(`Failed to submit request: ${e.message}`)
    throw e
  }
}

/**
 * Get the m2m token
 * @returns {String} the mem token
 */
async function getM2MToken () {
  return m2m.getMachineToken(config.AUTH0_CLIENT_ID, config.AUTH0_CLIENT_SECRET)
}

/**
 * Uses superagent to proxy patch request
 * @param {String} url the url
 * @param {Object} body the body
 * @param {String} m2mToken the m2m token
 * @returns {Object} the response
 */
async function patchRequest (url, body, m2mToken) {
  return request
    .patch(url)
    .send(body)
    .set('Authorization', `Bearer ${m2mToken}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
}

/**
 * Uses superagent to proxy get request
 * @param {String} url the url
 * @param {String} m2mToken the M2M token
 * @returns {Object} the response
 */
async function getRequest (url, m2mToken) {
  return request
    .get(url)
    .set('Authorization', `Bearer ${m2mToken}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
}

/**
 * Uses superagent to proxy put request
 * @param {String} url the url
 * @param {Object} body the body
 * @param {String} m2mToken the M2M token
 * @returns {Object} the response
 */
async function putRequest (url, body, m2mToken) {
  return request
    .put(url)
    .send(body)
    .set('Authorization', `Bearer ${m2mToken}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
}

/**
 * Uses superagent to proxy post request
 * @param {String} url the url
 * @param {Object} body the body
 * @param {String} m2mToken the M2M token
 * @returns {Object} the response
 */
async function postRequest (url, body, m2mToken) {
  return request
    .post(url)
    .send(body)
    .set('Authorization', `Bearer ${m2mToken}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
}

/**
 * Function to get userId
 * @param {String} handle the user's handle
 * @returns {String} the userId
 */
async function getUserId (handle) {
  const token = await getM2MToken()
  const url = `${config.TC_API}/members/${handle}?fields=userId`

  const res = await request
    .get(url)
    .set('Authorization', `Bearer ${token}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
  return res.body.userId
}

/**
 * Function to get copilot id
 * @param {String} challengeId the challengeId
 * @returns {String} the userId
 */
async function getCopilotId (challengeId) {
  const token = await getM2MToken()
  const url = `${config.TC_API}/resources?challengeId=${challengeId}&roleId=${config.COPILOT_ROLE_ID}`
  const res = await request
    .get(url)
    .set('Authorization', `Bearer ${token}`)
    .set('Content-Type', 'application/json')
    .set('Accept', 'application/json')
  return _.get(_.head(res.body), 'memberId')
}

/**
 * Creates a delay promise
 * @param {Number} ms the milliseconds to delay
 * @returns a promise
 */
function delay (ms) {
  return new Promise(resolve => {
      setTimeout(resolve, ms)
  })
}

/**
 * Generate a random integer
 * @param {Number} min the min
 * @param {Number} max the max
 * @returns the random integer
 */
function getRandomInt(min, max) {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min + 1)) + min
}

module.exports = {
  getInformixConnection,
  getKafkaOptions,
  patchRequest,
  getRequest,
  putRequest,
  postRequest,
  getUserId,
  getCopilotId,
  delay,
  getRandomInt,
  submitZendeskRequest
}

/**
 * Payment Service
 * Interacts with InformixDB
 */
const util = require('util')
const logger = require('../common/logger')
const helper = require('../common/helper')
const config = require('config');
const IDGenerator = require('../common/idGenerator')

// the paymentDetailId's generator
const paymentDetailIdGen = new IDGenerator('PAYMENT_DETAIL_SEQ')
// the paymentId's generator
const paymentIdGen = new IDGenerator('PAYMENT_SEQ')

// TODO - NOT checking on pd.net_amount = %d for now because you could edit an amount it'd create another payment.
const QUERY_PAYMENT = `SELECT 
p.payment_id, pd.payment_detail_id, pd.payment_desc, pd.payment_type_id, pd.payment_method_id, 
p.create_date, pd.create_date as modify_date, pd.net_amount, 
pd.payment_status_id, p.user_id,
pd.date_modified, pd.date_paid, pd.gross_amount,
pd.parent_payment_id, pd.total_amount, pd.installment_number, pd.jira_issue_id
FROM payment p
INNER JOIN payment_detail pd ON pd.payment_detail_id = p.most_recent_detail_id
WHERE
  p.user_id = %d
  AND pd.jira_issue_id = '%s'
  AND pd.payment_type_id = %d
`
// the insert statement of payment detail
const INSERT_PAYMENT_DETAIL = `INSERT INTO payment_detail (
  payment_detail_id, net_amount,  gross_amount, payment_status_id, modification_rationale_id, 
  payment_desc, payment_type_id, date_modified, date_due, payment_method_id, component_project_id, 
  create_date, charity_ind, total_amount, installment_number, create_user, 
  jira_issue_id) VALUES(?,?,?,?,?,?,?, CURRENT, EXTEND(CURRENT + INTERVAL (15) DAY(5) TO DAY, YEAR TO DAY),?,?, CURRENT,?,?,?,?,?)`
// the insert statement of payment
const INSERT_PAYMENT = 'INSERT INTO payment (payment_id, user_id, most_recent_detail_id, create_date, modify_date, has_global_ad) VALUES(?,?,?, CURRENT, CURRENT, "f")'
// the insert statement of payment detail xref
const INSERT_PAYMENT_DETAIL_XREF = 'INSERT INTO payment_detail_xref (payment_id, payment_detail_id) VALUES(?,?)'

const INSERT_PAYMENT_STATUS_REASON_XREF = 'INSERT INTO payment_detail_status_reason_xref (payment_detail_id, payment_status_reason_id) VALUES(?,?)'

/**
 * Prepare Informix statement
 * @param {Object} connection the Informix connection
 * @param {String} sql the sql
 * @return {Object} Informix statement
 */
async function prepare(connection, sql) {
  logger.debug(`Preparing SQL ${sql}`)
  const stmt = await connection.prepareAsync(sql)
  return Promise.promisifyAll(stmt)
}

async function paymentExists(payment, connection) {
  let isNewConn = false
  if (!connection) {
    connection = await helper.getInformixConnection()
    isNewConn = true
  }
  try {
    const query = util.format(QUERY_PAYMENT, payment.memberId, payment.v5ChallengeId, payment.typeId)
    logger.debug(`Checking if paymentExists - ${query}`)
    return connection.queryAsync(query)
  } catch (e) {
    logger.error(`Error in 'paymentExists' ${e}`)
    throw e
  } finally {
    if (isNewConn) await connection.closeAsync()
  }
}

/**
 * Create payment and save it to db
 * @param {Object} payment the payment info
 */
async function createPayment(payment) {
  try {
    const connection = await helper.getInformixConnection()
    await connection.beginTransactionAsync()

    const paymentExists = await paymentExists(payment, connection)
    logger.debug(`Payment Exists Response: ${JSON.stringify(paymentExists)}`)
    if (!paymentExists || paymentExists.length === 0) {
      const paymentDetailId = await paymentDetailIdGen.getNextId()
      const paymentId = await paymentIdGen.getNextId()
      const insertDetail = await prepare(connection, INSERT_PAYMENT_DETAIL)
      await insertDetail.executeAsync([paymentDetailId, payment.amount, payment.amount, payment.statusId, payment.modificationRationaleId, payment.desc, payment.typeId, payment.methodId, payment.projectId, payment.charityInd, payment.amount, payment.installmentNumber, payment.createUser, payment.v5ChallengeId])
      const insertPayment = await prepare(connection, INSERT_PAYMENT)
      await insertPayment.executeAsync([paymentId, payment.memberId, paymentDetailId])
      const insertDetailXref = await prepare(connection, INSERT_PAYMENT_DETAIL_XREF)
      await insertDetailXref.executeAsync([paymentId, paymentDetailId])
      const insertStatusXref = await prepare(connection, INSERT_PAYMENT_STATUS_REASON_XREF)
      await insertStatusXref.executeAsync([paymentDetailId, config.V5_PAYMENT_DETAIL_STATUS_REASON_ID])
      logger.info(`Payment ${paymentId} with detail ${paymentDetailId} has been inserted`)
      await connection.commitTransactionAsync()
    } else {
      logger.error(`Payment Exists for ${payment.v5ChallengeId}, skipping - ${JSON.stringify(paymentExists)}`)
      await connection.commitTransactionAsync()
    }
  } catch (e) {
    logger.error(`Error in 'createPayment' ${e}, rolling back transaction`)
    await connection.rollbackTransactionAsync()
    throw e
  } finally {
    await connection.closeAsync()
  }
}

module.exports = {
  createPayment,
  paymentExists
}

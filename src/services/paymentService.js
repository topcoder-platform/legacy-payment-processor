/**
 * Payment Service
 * Interacts with InformixDB
 */
const logger = require('../common/logger')
const helper = require('../common/helper')
const IDGenerator = require('../common/idGenerator')

// the paymentDetailId's generator
const paymentDetailIdGen = new IDGenerator('PAYMENT_DETAIL_SEQ')
// the paymentId's generator
const paymentIdGen = new IDGenerator('PAYMENT_SEQ')

// the insert statement of payment detail
const INSERT_PAYMENT_DETAIL = 'INSERT INTO payment_detail (payment_detail_id, net_amount,  gross_amount, payment_status_id, modification_rationale_id, payment_desc, payment_type_id, date_modified, date_due, payment_method_id, component_project_id, create_date, charity_ind, total_amount, installment_number, create_user) VALUES(?,?,?,?,?,?,?, CURRENT, CURRENT + INTERVAL (15) DAY(5) TO DAY,?,?, CURRENT,?,?,?,?)'
// the insert statement of payment
const INSERT_PAYMENT = 'INSERT INTO payment (payment_id, user_id, most_recent_detail_id, create_date, modify_date, has_global_ad) VALUES(?,?,?, CURRENT, CURRENT, false)'
// the insert statement of payment detail xref
const INSERT_PAYMENT_DETAIL_XREF = 'INSERT INTO payment_detail_xref (payment_id, payment_detail_id) VALUES(?,?)'

/**
 * Prepare Informix statement
 * @param {Object} connection the Informix connection
 * @param {String} sql the sql
 * @return {Object} Informix statement
 */
async function prepare (connection, sql) {
  logger.debug(`Preparing SQL ${sql}`)
  const stmt = await connection.prepareAsync(sql)
  return Promise.promisifyAll(stmt)
}

/**
 * Create payment and save it to db
 * @param {Object} payment the payment info
 */
async function createPayment (payment) {
  const connection = await helper.getInformixConnection()
  const paymentDetailId = await paymentDetailIdGen.getNextId()
  const paymentId = await paymentIdGen.getNextId()
  try {
    await connection.beginTransactionAsync()
    const insertDetail = await prepare(connection, INSERT_PAYMENT_DETAIL)
    await insertDetail.executeAsync([paymentDetailId, payment.amount, payment.amount, payment.statusId, payment.modificationRationaleId, payment.desc, payment.typeId, payment.methodId, payment.projectId, payment.charityInd, payment.amount, payment.installmentNumber, payment.createUser])
    const insertPayment = await prepare(connection, INSERT_PAYMENT)
    await insertPayment.executeAsync([paymentId, payment.memberId, paymentDetailId])
    const insertDetailXref = await prepare(connection, INSERT_PAYMENT_DETAIL_XREF)
    await insertDetailXref.executeAsync([paymentId, paymentDetailId])
    logger.info(`Payment ${paymentId} with detail ${paymentDetailId} has been inserted`)
    await connection.commitTransactionAsync()
  } catch (e) {
    logger.error(`Error in 'createPayment' ${e}, rolling back transaction`)
    await connection.rollbackTransactionAsync()
    throw e
  } finally {
    await connection.closeAsync()
  }
}

module.exports = {
  createPayment
}

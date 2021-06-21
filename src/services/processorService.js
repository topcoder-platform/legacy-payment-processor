/**
 * Processor Service
 * Processes messages gathered from Kafka
 */

const _ = require('lodash')
const Joi = require('@hapi/joi')
const logger = require('../common/logger')
const helper = require('../common/helper')
const paymentService = require('./paymentService')
const config = require('config')

/**
 * Process update challenge message
 * @param {Object} message the kafka message
 */
async function processUpdate(message) {
  const createUserId = await helper.getUserId(message.payload.createdBy)
  const legacyId = _.get(message, 'payload.legacyId', null)
  const v5ChallengeId = _.get(message, 'payload.id', null)

  if (!v5ChallengeId || v5ChallengeId === '') {
    logger.error('Payload of challenge does not contain a v5 Challenge UUID')
    return false
  }
  //const grossAmount = _.sumBy(_.flatMap(message.payload.prizeSets, 'prizes'), 'value')

  // the same properties of userPayment and copilotPayment
  const basePayment = {
    statusId: config.PAYMENT_STATUS_ID,
    modificationRationaleId: config.MODIFICATION_RATIONALE_ID,
    methodId: config.PAYMENT_METHOD_ID,
    projectId: legacyId, // this is not projectId from v5 - legacy calls a challenge a project
    charityInd: config.CHARITY_IND,
    installmentNumber: config.INSTALLMENT_NUMBER,
    createUser: createUserId,
    v5ChallengeId
  }

  // add winner payment
  try {
    const winnerPrizes = _.get(_.find(message.payload.prizeSets, ['type', 'placement']), 'prizes', [])
    // const winnerPaymentDesc = _.get(_.find(message.payload.prizeSets, ['type', 'placement']), 'description', '')
    const winnerMembers = _.sortBy(_.get(message.payload, 'winners', []), ['placement'])
    if (_.isEmpty(winnerPrizes)) {
      logger.warn(`For challenge ${v5ChallengeId}, no winner payment avaiable`)
    } else if (winnerPrizes.length !== winnerMembers.length) {
      logger.error(`For challenge ${v5ChallengeId}, there is ${winnerPrizes.length} user prizes but ${winnerMembers.length} winners`)
    } else {
      try {
        for (let i = 1; i <= winnerPrizes.length; i++) {
          const payment = _.assign({
            memberId: winnerMembers[i - 1].userId,
            amount: winnerPrizes[i - 1].value,
            desc: `Task - ${message.payload.name} - ${i} Place`,
            typeId: config.WINNER_PAYMENT_TYPE_ID
          }, basePayment)

          const paymentExists = await paymentService.paymentExists(payment)
          logger.debug(`Payment Exists Response: ${JSON.stringify(paymentExists)}`)
          if(!paymentExists || paymentExists.length === 0) {
            await paymentService.createPayment(payment)
          } else {
            logger.error(`Payment Exists for ${v5ChallengeId}, skipping - ${JSON.stringify(paymentExists)}`)
          }
        }
      } catch (error) {
        logger.error(`For challenge ${v5ChallengeId}, add winner payments error: ${error}`)
      }
    }

    // add copilot payment
    const copilotId = await helper.getCopilotId(message.payload.id)
    const copilotAmount = _.get(_.head(_.get(_.find(message.payload.prizeSets, ['type', 'copilot']), 'prizes', [])), 'value')
    const copilotPaymentDesc = _.get(_.find(message.payload.prizeSets, ['type', 'copilot']), 'description', '')

    if (!copilotAmount) {
      logger.warn(`For challenge ${v5ChallengeId}, no copilot payment available`)
    } else if (!copilotId) {
      logger.warn(`For challenge ${v5ChallengeId}, no copilot memberId available`)
    } else {
      try {
        const copilotPayment = _.assign({
          memberId: copilotId,
          amount: copilotAmount,
          desc: (copilotPaymentDesc ? copilotPaymentDesc : `Task - ${message.payload.name} - Copilot`),
          typeId: config.COPILOT_PAYMENT_TYPE_ID
        }, basePayment)
        const paymentExists = await paymentService.paymentExists(copilotPayment)
        logger.debug(`Copilot Payment Exists Response: ${JSON.stringify(paymentExists)}`)
        if(!paymentExists || paymentExists.length === 0) {
          await paymentService.createPayment(copilotPayment)
        } else {
          logger.error(`Copilot Payment Exists for ${v5ChallengeId}, skipping - ${JSON.stringify(paymentExists)}`)
        }
      } catch (error) {
        logger.error(`For challenge ${v5ChallengeId}, add copilot payments error: ${error}`)
      }
    }
  } catch (error) {
    logger.error(`For challenge ${v5ChallengeId}, error occurred while parsing and preparing payment detail. Error: ${error}`)
  }
}

processUpdate.schema = {
  message: Joi.object().keys({
    topic: Joi.string().required(),
    originator: Joi.string().required(),
    timestamp: Joi.date().required(),
    'mime-type': Joi.string().required(),
    payload: Joi.object().keys({
      id: Joi.string().required(),
      legacyId: Joi.number().integer().positive(),
      task: Joi.object().keys({
        memberId: Joi.string().allow(null)
      }).unknown(true).required(),
      name: Joi.string().required(),
      prizeSets: Joi.array().items(Joi.object().keys({
        type: Joi.string().valid('copilot', 'placement').required(),
        prizes: Joi.array().items(Joi.object().keys({
          value: Joi.number().positive().required()
        }).unknown(true))
      }).unknown(true)).min(1),
      winners: Joi.array().items(Joi.object({
        userId: Joi.number().integer().positive().required(),
        handle: Joi.string(),
        placement: Joi.number().integer().positive().required()
      }).unknown(true)),
      type: Joi.string().required(),
      status: Joi.string().required(),
      createdBy: Joi.string().required()
    }).unknown(true).required()
  }).required()
}

module.exports = {
  processUpdate
}

logger.buildService(module.exports)

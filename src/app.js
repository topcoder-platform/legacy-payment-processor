/**
 * The application entry point
 */
global.Promise = require('bluebird')
const _ = require('lodash')
const config = require('config')
const Kafka = require('no-kafka')
const healthcheck = require('@topcoder-platform/topcoder-healthcheck-dropin')
const logger = require('./common/logger')
const helper = require('./common/helper')
const processorService = require('./services/processorService')

// Start kafka consumer
logger.info('Starting kafka consumer')
// create consumer
const consumer = new Kafka.GroupConsumer(helper.getKafkaOptions())

/*
 * Data handler linked with Kafka consumer
 * Whenever a new message is received by Kafka consumer,
 * this function will be invoked
 */
const dataHandler = (messageSet, topic, partition) => Promise.each(messageSet, async (m) => {
  const message = m.message.value.toString('utf8')
  logger.info(`Handle Kafka event message; Topic: ${topic}; Partition: ${partition}; Offset: ${
    m.offset}; Message: ${message}.`)
  let messageJSON
  try {
    messageJSON = JSON.parse(message)
  } catch (e) {
    logger.error('Invalid message JSON.')
    logger.logFullError(e)

    // commit the message and ignore it
    await consumer.commitOffset({ topic, partition, offset: m.offset })
    return
  }

  if (messageJSON.topic !== topic) {
    logger.error(`The message topic ${messageJSON.topic} doesn't match the Kafka topic ${topic}.`)

    // commit the message and ignore it
    await consumer.commitOffset({ topic, partition, offset: m.offset })
    return
  }

  const timelineTemplateId = _.get(messageJSON.payload, 'timelineTemplateId');

  // Currently only process payments for challenges with `legacy.pureV5Task: true` or `legacy.pureV5: true`
  if (!_.get(messageJSON.payload, 'legacy.pureV5Task', false) && !_.get(messageJSON.payload, 'legacy.pureV5', false) && timelineTemplateId != config.get('TOPCROWD_CHALLENGE_TEMPLATE_ID')) {
    logger.info(`Challenge Legacy Object ${JSON.stringify(_.get(messageJSON.payload, 'legacy'))} does not have legacy.pureV5Task: true or legacy.pureV5: true or timelineTemplateId: ${timelineTemplateId}.`)
    await consumer.commitOffset({ topic, partition, offset: m.offset })
    return
  }

  if (_.toUpper(_.get(messageJSON.payload, 'status')) !== 'COMPLETED') {
    logger.info(`The message type ${_.get(messageJSON.payload, 'type')}, status ${_.get(messageJSON.payload, 'status')} doesn't match {status: 'Completed'}.`)

    // commit the message and ignore it
    await consumer.commitOffset({ topic, partition, offset: m.offset })
    return
  }

  try {
    // delay for a random amount of time between 5-20 sec
    // to minimize the chance of having two processes doing the same at the same time
    await helper.delay(helper.getRandomInt(5 * 1000, 20 * 1000))
    await processorService.processUpdate(messageJSON)
    logger.debug('Successfully processed message')
  } catch (err) {
    logger.logFullError(err)
  } finally {
    // Commit offset regardless of error
    await consumer.commitOffset({ topic, partition, offset: m.offset })
  }
})

// check if there is kafka connection alive
const check = () => {
  if (!consumer.client.initialBrokers && !consumer.client.initialBrokers.length) {
    return false
  }
  let connected = true
  consumer.client.initialBrokers.forEach(conn => {
    logger.debug(`url ${conn.server()} - connected=${conn.connected}`)
    connected = conn.connected & connected
  })
  return connected
}

const topics = [config.UPDATE_CHALLENGE_TOPIC]

consumer
  .init([{
    subscriptions: topics,
    handler: dataHandler
  }])
  // consume configured topics
  .then(() => {
    logger.info('Initialized.......')
    healthcheck.init([check])
    logger.info('Adding topics successfully.......')
    logger.info(topics)
    logger.info('Kick Start.......')
  })
  .catch((err) => logger.error(err))

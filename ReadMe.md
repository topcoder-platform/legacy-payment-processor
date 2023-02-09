# Topcoder - Legacy Payment Processor

This microservice processes kafka events related to challenges and backfills data via V5 Challenge API.

## Prerequisites
- [NodeJS](https://nodejs.org/en/) (v10)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Kafka](https://kafka.apache.org/)
- [Informix](https://www.ibm.com/products/informix)

## Configuration

Configuration for the legacy challenge processor is at `config/default.js`.
The following parameters can be set in config files or in env variables:
- LOG_LEVEL: the log level; default value: 'debug'
- KAFKA_URL: comma separated Kafka hosts; default value: 'localhost:9092'
- KAFKA_CLIENT_CERT: Kafka connection certificate, optional; default value is undefined;
    if not provided, then SSL connection is not used, direct insecure connection is used;
    if provided, it can be either path to certificate file or certificate content
- KAFKA_CLIENT_CERT_KEY: Kafka connection private key, optional; default value is undefined;
    if not provided, then SSL connection is not used, direct insecure connection is used;
    if provided, it can be either path to private key file or private key content
- KAFKA_GROUP_ID: the Kafka group id, default value is 'legacy-payment-processor'
- KAFKA_ERROR_TOPIC: The kafka error topic.
- UPDATE_CHALLENGE_TOPIC: the update challenge Kafka message topic, default value is 'challenge.notification.update'
- AUTH0_URL: Auth0 URL, used to get TC M2M token
- AUTH0_AUDIENCE: Auth0 audience, used to get TC M2M token
- TOKEN_CACHE_TIME: Auth0 token cache time, used to get TC M2M token
- AUTH0_CLIENT_ID: Auth0 client id, used to get TC M2M token
- AUTH0_CLIENT_SECRET: Auth0 client secret, used to get TC M2M token
- AUTH0_PROXY_SERVER_URL: Proxy Auth0 URL, used to get TC M2M token
- TC_API: The Topcoder v5 url, default value is https://api.topcoder-dev.com/v5
There is a `/health` endpoint that checks for the health of the app. This sets up an expressjs server and listens on the environment variable `PORT`. It's not part of the configuration file and needs to be passed as an environment variable
- INFORMIX.SERVER: The informix server, default value is 'informixoltp_tcp'
- INFORMIX.DATABASE: The informix database, default value is 'tcs_catalog'
- INFORMIX.HOST: The informix host, default value is 'localhost'
- INFORMIX.PROTOCOL: The informix protocol, default value is 'onsoctcp'
- INFORMIX.PORT: The informix port, default value is '2021'
- INFORMIX.DB_LOCALE: The informix db locale, default value is 'en_US.57372'
- INFORMIX.USER: The informix user, default value is 'informix'
- INFORMIX.PASSWORD: The informix password, default value is '1nf0rm1x'
- INFORMIX.POOL_MAX_SIZE: The informix connection pool max size, default value is '60'
- PAYMENT_STATUS_ID: The payment status id, default value is '70'
- MODIFICATION_RATIONALE_ID: The payment modification rationale id, default value is '1'
- WINNER_PAYMENT_TYPE_ID: The winner payment type id, default value is '65'
- COPILOT_PAYMENT_TYPE_ID: The copilot payment type id, default value is '45'
- PAYMENT_METHOD_ID: The payment method id, default value is '1'
- CHARITY_IND: The payment charity ind, default value is '0'
- INSTALLMENT_NUMBER: The payment installment number, default value is '1'

## Local Kafka setup

- `http://kafka.apache.org/quickstart` contains details to setup and manage Kafka server,
  below provides details to setup Kafka server in Linux/Mac, Windows will use bat commands in bin/windows instead
- download kafka at `https://www.apache.org/dyn/closer.cgi?path=/kafka/1.1.0/kafka_2.11-1.1.0.tgz`
- extract out the downloaded tgz file
- go to extracted directory kafka_2.11-0.11.0.1
- start ZooKeeper server:
  `bin/zookeeper-server-start.sh config/zookeeper.properties`
- use another terminal, go to same directory, start the Kafka server:
  `bin/kafka-server-start.sh config/server.properties`
- note that the zookeeper server is at localhost:2181, and Kafka server is at localhost:9092
- use another terminal, go to same directory, create the needed topics:
  `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic challenge.notification.update`

- verify that the topics are created:
  `bin/kafka-topics.sh --list --zookeeper localhost:2181`,
  it should list out the created topics
- run the producer and then write some message into the console to send to the `challenge.notification.update` topic:
  `bin/kafka-console-producer.sh --broker-list localhost:9092 --topic challenge.notification.update`
  in the console, write message, one message per line:
  `{"topic":"challenge.notification.update","originator":"topcoder-challenges-api","timestamp":"2019-05-14T00:00:00.000Z","mime-type":"application/json","payload":{"projectId":16640,"terms":[{"id":"317cd8f9-d66c-4f2a-8774-63c612d99cd4","roleId":"732339e7-8e30-49d7-9198-cccf9451e221"}],"task":{"isTask":true,"isAssigned":true,"memberId":"40029484"},"typeId":"ecd58c69-238f-43a4-a4bb-d172719b9f31","legacyId":30058134,"endDate":"2021-01-30T02:09:47.000Z","status":"Completed","trackId":"9b6fc876-f4d9-4ccb-9dfd-419247628825","created":"2021-01-28T02:09:48.677Z","updatedBy":"tcwebservice","createdBy":"jcori","name":"Test Task for Sync","timelineTemplateId":"53a307ce-b4b3-4d6f-b9a1-3741a58f77e6","groups":[],"descriptionFormat":"markdown","updated":"2021-01-28T02:10:55.674Z","phases":[{"phaseId":"6950164f-3c5e-4bdc-abc8-22aaf5a1bd49","duration":86400,"id":"af753aa9-ac3d-4321-b2a8-f183d48cacfe","name":"Submission","isOpen":false,"scheduledStartDate":"2021-01-29T02:09:47.000Z","scheduledEndDate":"2021-01-30T02:09:47.000Z","actualStartDate":"2021-01-29T02:09:47.000Z","actualEndDate":"2021-01-30T02:09:47.000Z"},{"phaseId":"003a4b14-de5d-43fc-9e35-835dbeb6af1f","duration":86400,"id":"bb743041-1226-4686-969e-69f90a01c7fa","name":"Iterative Review","isOpen":false,"predecessor":"af753aa9-ac3d-4321-b2a8-f183d48cacfe","scheduledStartDate":"2021-01-30T02:09:47.000Z","scheduledEndDate":"2021-01-31T02:09:47.000Z","actualStartDate":"2021-01-30T02:09:47.000Z","actualEndDate":"2021-01-31T02:09:47.000Z"},{"phaseId":"ad985cff-ad3e-44de-b54e-3992505ba0ae","duration":86400,"id":"30f0573f-9dc2-4117-825e-ebbf40b96852","name":"Approval","isOpen":false,"predecessor":"bb743041-1226-4686-969e-69f90a01c7fa","scheduledStartDate":"2021-01-31T02:09:47.000Z","scheduledEndDate":"2021-02-01T02:09:47.000Z","actualStartDate":"2021-01-31T02:09:47.000Z","actualEndDate":"2021-02-01T02:09:47.000Z"}],"prizeSets":[{"type":"copilot","prizes":[{"type":"USD","value":1}]},{"type":"placement","prizes":[{"type":"USD","value":10}]}],"startDate":"2021-01-29T02:09:47.000Z","legacy":{"track":"DEVELOP","subTrack":"FIRST_2_FINISH","reviewType":"INTERNAL","confidentialityType":"public","forumId":0,"directProjectId":23678,"isTask":true},"description":"Test","id":"77e031dd-bd35-45f3-bad8-4cc41dbc282f","tags":["Google"],"track":"Development","type":"Task"}}`
- optionally, use another terminal, go to same directory, start a consumer to view the messages:
  `bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic challenge.notification.update --from-beginning`

## Local informix setup

- Follow the below instructions to setup informix:

```
$ docker pull appiriodevops/informix
$ docker run --name tc-informix -p 2021:2021 -it appiriodevops/informix
```

- After this go to the docker terminal by runing: docker exec -it tc-informix bash
- Switch to informix user: sudo su - informix
- Start the db ( https://i.imgur.com/oTeuhOJ.png ) by runing: ./start-informix.sh

## Local deployment without Docker

Please make sure you installed and configured kafka and informix

Install all dependencies

> Note: ifxnjs doesn't support macos, so it'll install fail. you can't deploy on macos without docker.

```
npm install
```

Run the lint

```
npm run lint
```

Set environment variables for M2M Token

```
export AUTH0_CLIENT_ID=jGIf2pd3f44B1jqvOai30BIKTZanYBfU
export AUTH0_CLIENT_SECRET=ldzqVaVEbqhwjM5KtZ79sG8djZpAVK8Z7qieVcC3vRjI4NirgcinKSBpPwk6mYYP
```

Run the application

```
npm start
```

### Local Deployment with Nodejs app and informix in Docker, and Kafka in local machine

1. Make sure that Kafka in running as instructions above.

2. Go to `docker` folder

3. Create `.env` file And add below environment for the variables, Please see that 192.168.1.3 is the IP of host machine for docker where we run all the dependencie

```
AUTH0_CLIENT_ID=jGIf2pd3f44B1jqvOai30BIKTZanYBfU
AUTH0_CLIENT_SECRET=ldzqVaVEbqhwjM5KtZ79sG8djZpAVK8Z7qieVcC3vRjI4NirgcinKSBpPwk6mYYP
KAFKA_URL=192.168.1.3:9092
INFORMIX_HOST=192.168.1.3
```

If the above don't work, update these configs in `config/default.js`

4. Once that is done, go to run the following command

```
docker-compose up
```

5. When you are running the application for the first time, It will take some time initially to download the image and install the dependencies

### Local Deployment with complete setup in Docker

- For changes made to source-code to be reflected in the docker image, delete the container, image and volume for a fresh build

1. Create a network in docker using `docker network create <network-name>`, for example `docker network create lpp`

2. Use the network created above, in `/local/docker-compose.yml` file by adding the following section:
```
networks:
  default:
    external:
      name: lpp
```

3. Start the dependencies(zookeeper, kafka and informix) in docker by navigating to `local directory` and issuing `docker-compose up`

4. Modify the following in config parameters `config/default.js`:
```
KAFKA_URL: process.env.KAFKA_URL || 'kafka:9092'
INFORMIX.HOST: process.env.INFORMIX_HOST || 'informix'
```

5. Go to the `docker directory` and create an `.env` file with the following configuration:
```
AUTH0_CLIENT_ID=jGIf2pd3f44B1jqvOai30BIKTZanYBfU
AUTH0_CLIENT_SECRET=ldzqVaVEbqhwjM5KtZ79sG8djZpAVK8Z7qieVcC3vRjI4NirgcinKSBpPwk6mYYP
KAFKA_URL=kafka:9092
INFORMIX_HOST=informix
```

6. Use the network created above, in `/docker/docker-compose.yml` file by adding the following section:
```
networks:
  default:
    external:
      name: lpp
```

7. Start the app in docker by issuing `docker-compose up` in the `docker directory`

8. When you are running the application for the first time, It will take some time initially to download the image and install the dependencies

9. For sending messages to kafka, open a terminal and issue `docker exec -it kafkaC /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9093 --topic challenge.notification.update` Once the producer is up and running, send events as required.

## Verification
Refer to the verification document `Verification.md`

## Notes

you need to use correct createdBy of kafka message values for verification.

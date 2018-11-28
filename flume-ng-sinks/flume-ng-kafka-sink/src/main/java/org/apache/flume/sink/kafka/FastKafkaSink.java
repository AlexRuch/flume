/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * limitations under the License.
 */

package org.apache.flume.sink.kafka;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class FastKafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    private String topic;
    private volatile KafkaSinkCounter counter;
    private int batchSize;


    private static int numberOfThreads;
    private static KafkaSink[] producerArr;
    private static KafkaProducer[] kafkaProducers;
    private static Properties props;
    private static String[] messagesQueue;
    private KafkaSink deadThread = null;
    private KafkaSink newKafkaSink = null;
    private boolean taskInWork;
    private int producerIndex = 0;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        messagesQueue = new String[batchSize];
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            taskInWork = false;
            int queueNum = 0;
            while (queueNum < messagesQueue.length) {
                event = channel.take();
                if (event != null) {
                    messagesQueue[queueNum] = new String(event.getBody());
                    queueNum++;
                }
            }

            taskInWork = false;
            while (!taskInWork) {
                for (int i = 0; i < numberOfThreads; i++) {
                    if (!producerArr[i].isAlive()) {
                        deadThread = producerArr[i];
                        producerIndex = i;
                        newKafkaSink = new KafkaSink(messagesQueue, deadThread.getProducer(), deadThread.getName());
                        newKafkaSink.start();
                        taskInWork = true;
                        transaction.commit();
                        break;
                    }
                }
            }

            producerArr[producerIndex] = newKafkaSink;

        } catch (
                Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
                    counter.incrementRollbackCount();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }
        return result;
    }

    @Override
    public synchronized void start() {
        // instantiate the producer
        counter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        for(KafkaProducer producer : kafkaProducers){
            producer.close();
        }
        counter.stop();
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }


    /**
     * We configure the sink and generate properties for the Kafka Producer
     * <p>
     * Kafka producer properties is generated as follows:
     * 1. We generate a properties object with some static defaults that
     * can be overridden by Sink configuration
     * 2. We add the configuration users added for Kafka (parameters starting
     * with .kafka. and must be valid Kafka Producer properties
     * 3. We add the sink's documented parameters which can override other
     * properties
     *
     * @param context
     */
    @Override
    public void configure(Context context) {


        numberOfThreads = context.getInteger(KafkaSinkConstants.THREADS_NUMBER, KafkaSinkConstants.DEFAULT_THREADS_NUMBER);
        props = KafkaSinkUtil.getKafkaProperties(context);
        batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE,
                KafkaSinkConstants.DEFAULT_BATCH_SIZE);

        kafkaProducers = new KafkaProducer[numberOfThreads];
        producerArr = new KafkaSink[numberOfThreads];
        messagesQueue = new String[batchSize];

        logger.debug("BATCH SIZE: " + batchSize);

        for (int i = 0; i < numberOfThreads; i++) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(props);
            producerArr[i] = (new KafkaSink(messagesQueue, producer, Integer.toString(i + 1)));
        }

        logger.debug("Using batch size: {}", batchSize);


        topic = context.getString(KafkaSinkConstants.TOPIC,
                KafkaSinkConstants.DEFAULT_TOPIC);

        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
            logger.warn("The Property 'topic' is not set. " +
                    "Using the default topic name: " +
                    KafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            logger.info("Using the static topic: " + topic +
                    " this may be over-ridden by event headers");
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }


    }

    private Lock lock = new ReentrantLock();

    public class KafkaSink extends Thread {

        private String[] kafkaSinkQueue;
        private KafkaProducer<String, String> producer;

        KafkaSink(String[] kafkaSinkQueue, KafkaProducer<String, String> producer, String threadName) {
            this.kafkaSinkQueue = kafkaSinkQueue;
            this.producer = producer;
            super.setName(threadName);
        }

        @Override
        public synchronized void run() {
            long startTime = System.nanoTime();
            for (String aKafkaSinkQueue : kafkaSinkQueue) {
                producer.send(new ProducerRecord<>(topic, aKafkaSinkQueue));
            }
            producer.flush();
            long endTime = System.nanoTime();
            try {
                lock.lock();
                counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount((long) kafkaSinkQueue.length);
            } finally {
                lock.unlock();
            }
        }

        KafkaProducer<String, String> getProducer() {
            return producer;
        }
    }
}

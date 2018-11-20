package org.apache.flume.sink.kafka;

import com.google.common.base.Throwables;
import org.apache.avro.util.Utf8;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FastKafkaSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private int count = 0;
    private static KafkaProducer<String, String> producer;
    private List<String> messagesList = new ArrayList<>();
    private KafkaSinkCounter counter;
    private ExecutorService executor = Executors.newCachedThreadPool();

    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("buffer.memory", "67108864");
        props.put("batch.size", "20000");
        props.put("transactional.id", "my-transactional-id");
        producer = new KafkaProducer<>(props);
    }

    @Override
    public Status process() throws EventDeliveryException {
        Transaction transaction = null;
        Status result = Status.READY;

        try {

            Channel channel = getChannel();
            transaction = channel.getTransaction();
            transaction.begin();
            while (messagesList.size() < 10000){
                if (channel.take() != null) {
                    messagesList.add(new String(channel.take().getBody(), StandardCharsets.UTF_8));
                }
            }
//            new Thread(new KafkaProducerThread(messagesList, producer)).start();
            executor.submit(new KafkaProducerThread(messagesList));
            messagesList = new ArrayList<>();
            transaction.commit();

//            try {
//                if (channel.take().getBody() != null) {
//
//                        messagesList.add(new String(channel.take().getBody(), StandardCharsets.UTF_8));
//                    } else {
//                        logger.info(String.valueOf(messagesList.size()));
//                        new Thread(new KafkaProducerThread(messagesList, producer)).start();
//                        messagesList = new LinkedList<>();
//                    }
//                }
//            } catch (NullPointerException e) {
//
//            }


        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
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
    public void configure(Context context) {

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }

    }

    private class KafkaProducerThread implements Runnable {

        private List<String> messagesList;
//        private KafkaProducer<String, String> producer;


//        KafkaProducerThread(List<String> messagesList, KafkaProducer<String, String> producer) {
//            this.messagesList = messagesList;
//            this.producer = producer;
//        }

        KafkaProducerThread(List<String> messagesList) {
            this.messagesList = messagesList;
        }

        @Override
        public void run() {
            logger.info("START MESSAGING TO KAFKA : " + messagesList.size());
            messagesList.forEach(message -> producer.send(new ProducerRecord<>("fastkafka", message)));
            producer.flush();
            counter.addToEventDrainSuccessCount((long) messagesList.size());
        }
    }
}

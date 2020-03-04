package com.alibaba.otter.canal.kafka;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.alibaba.otter.canal.common.MQFlatFlatMessageUtils;
import com.alibaba.otter.canal.protocol.FlatFlatMessage;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.MQMessageUtils;
import com.alibaba.otter.canal.common.MQProperties;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.spi.CanalMQProducer;

/**
 * kafka producer 主操作类
 *
 * @author machengyuan 2018-6-11 下午05:30:49
 * @version 1.0.0
 */
public class CanalKafkaProducer implements CanalMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(CanalKafkaProducer.class);

//    private Producer<String, Message> producer;
//    private Producer<String, String> producer2;                                                 // 用于扁平message的数据投递
//    private MQProperties kafkaProperties;

    @Override
    public void init(MQProperties kafkaProperties) {

    }

    @Override
    public void stop() {
        try {
            logger.info("## stop the kafka producer");

        } catch (Throwable e) {
            logger.warn("##something goes wrong when stopping kafka producer:", e);
        } finally {
            logger.info("## kafka producer is down.");
        }
    }

    @Override
    public void send(MQProperties.CanalDestination canalDestination, Message message, Callback callback) {
        try {
            if (!StringUtils.isEmpty(canalDestination.getDynamicTopic())) {
                // 动态topic
                Map<String, Message> messageMap = MQMessageUtils.messageTopics(message,
                        canalDestination.getTopic(),
                        canalDestination.getDynamicTopic());

                for (Map.Entry<String, Message> entry : messageMap.entrySet()) {
                    String topicName = entry.getKey(); //.replace('.', '_');
                    Message messageSub = entry.getValue();
                    if (logger.isDebugEnabled()) {
                        logger.debug("## Send message to kafka topic: " + topicName);
                    }
                    send(canalDestination, topicName, messageSub);
                }
            } else {
                send(canalDestination, canalDestination.getTopic(), message);
            }
            callback.commit();
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
            callback.rollback();
        }
    }

    private void send(MQProperties.CanalDestination canalDestination, String topicName, Message message)
            throws Exception {
        if (false) {
            List<ProducerRecord> records = new ArrayList<ProducerRecord>();
            if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
                Message[] messages = MQMessageUtils.messagePartition(message,
                        canalDestination.getPartitionsNum(),
                        canalDestination.getPartitionHash());
                int length = messages.length;
                for (int i = 0; i < length; i++) {
                    Message messagePartition = messages[i];
                    if (messagePartition != null) {
                        records.add(new ProducerRecord<String, Message>(topicName, i, null, messagePartition));
                    }
                }
            } else {
                final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
                records.add(new ProducerRecord<String, Message>(topicName, partition, null, message));
            }

            produce(topicName, records, false);
        } else {
            // 发送扁平数据json
            List<FlatFlatMessage> flatMessages = MQFlatFlatMessageUtils.messageConverter(message);
            /**
             * 新增发送方式
             * topic=prefix+"_"+dababasename
             * key=tablename+keysvalue
             * */
            if ("true".equals(canalDestination.getIsTablePkHash())) {
                if (flatMessages != null) {
                    for (FlatFlatMessage flatMessage : flatMessages) {
                        String key = "";
                        try {
                            key = flatMessage.getDatabase() + flatMessage.getTable() + flatMessage.getData().get(flatMessage.getPkNames().get(0));
                        } catch (Exception e) {
                            key = flatMessage.getDatabase() + flatMessage.getTable();
                        }
                        produceByKeyFlatFlatMessage(topicName, key, flatMessage);
                    }
                }
            }
        }
    }
//            // 发送扁平数据json
//            List<FlatMessage> flatMessages = MQMessageUtils.messageConverter(message);
//            List<ProducerRecord> records = new ArrayList<ProducerRecord>();
//            if (flatMessages != null) {
//                for (FlatMessage flatMessage : flatMessages) {
//                    if (canalDestination.getPartitionHash() != null && !canalDestination.getPartitionHash().isEmpty()) {
//                        FlatMessage[] partitionFlatMessage = MQMessageUtils.messagePartition(flatMessage,
//                                canalDestination.getPartitionsNum(),
//                                canalDestination.getPartitionHash());
//                        int length = partitionFlatMessage.length;
//                        for (int i = 0; i < length; i++) {
//                            FlatMessage flatMessagePart = partitionFlatMessage[i];
//                            if (flatMessagePart != null) {
//                                records.add(new ProducerRecord<String, String>(topicName,
//                                        i,
//                                        null,
//                                        JSON.toJSONString(flatMessagePart, SerializerFeature.WriteMapNullValue)));
//                            }
//                        }
//                    } else {
//                        final int partition = canalDestination.getPartition() != null ? canalDestination.getPartition() : 0;
//                        records.add(new ProducerRecord<String, String>(topicName,
//                                partition,
//                                null,
//                                JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue)));
//                    }
//
//                    // 每条记录需要flush
//                    produce(topicName, records, true);
//                    records.clear();
//                }
//            }


    private void produce(String topicName, List<ProducerRecord> records, boolean flatMessage) {



        List<Future> futures = new ArrayList<Future>();
        try {
            // 异步发送，因为在partition hash的时候已经按照每个分区合并了消息，走到这一步不需要考虑单个分区内的顺序问题
            for (ProducerRecord record : records) {
//                futures.add(producerTmp.send(record));
            }
        } finally {
            if (logger.isDebugEnabled()) {
                for (ProducerRecord record : records) {
                    logger.debug("Send  message to kafka topic: [{}], packet: {}", topicName, record.toString());
                }
            }
            // 批量刷出
//            producerTmp.flush();

            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接出发rollback
            for (Future future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void produceByKeyFlatFlatMessage(String topicName, String key, FlatFlatMessage flatFlatMessage) throws ExecutionException,
            InterruptedException {
//        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,
//                key,
//                JSON.toJSONString(flatFlatMessage, SerializerFeature.WriteMapNullValue));
//        producer2.send(record).get();
        System.out.println(JSON.toJSONString(flatFlatMessage));

    }

    private void produceByKeyFlatFlatMessages(String topicName, List<FlatFlatMessage> flatFlatMessages,MQProperties.CanalDestination canalDestination) throws ExecutionException,
            InterruptedException {

        List<Future> futures = new ArrayList<Future>();
        try {
            for (FlatFlatMessage flatMessage : flatFlatMessages) {
                MQFlatFlatMessageUtils.HashMode hashMode = MQFlatFlatMessageUtils.getPartitionHashColumns(flatMessage.getTable(), canalDestination.getPartitionHash());
                StringBuilder keyBuilder = new StringBuilder();
                //根据配置做key
                if(hashMode!=null){
                    keyBuilder.append(flatMessage.getDatabase());
                    keyBuilder.append(flatMessage.getTable());
                    for (String pkName : hashMode.pkNames) {
                        keyBuilder.append(flatMessage.getData().get(pkName));
                    }
                }else{
                    //按照表名+主键做key发送
                    keyBuilder.append(flatMessage.getDatabase());
                    keyBuilder.append(flatMessage.getTable());
                    if (flatMessage.getPkNames() != null) {
                        for (String pkName : flatMessage.getPkNames()) {
                            keyBuilder.append(flatMessage.getData().get(pkName));
                        }
                    }
                }
//                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,
//                        keyBuilder.toString(),
//                        JSON.toJSONString(flatMessage, SerializerFeature.WriteMapNullValue));
//                futures.add(producer2.send(record));
                System.out.println(JSON.toJSONString(flatMessage));
            }
        }finally {
            if (logger.isDebugEnabled()) {
                for (FlatFlatMessage record : flatFlatMessages) {
                    logger.debug("Send  message to kafka topic: [{}], packet: {}", topicName, record.toString());
                }
            }
            // 批量刷出
//            producer2.flush();
            // flush操作也有可能是发送失败,这里需要异步关注一下发送结果,针对有异常的直接出发rollback
            for (Future future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}

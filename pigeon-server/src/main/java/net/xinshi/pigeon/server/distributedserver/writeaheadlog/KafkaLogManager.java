package net.xinshi.pigeon.server.distributedserver.writeaheadlog;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaLogManager  implements ILogManager, Callback {
    KafkaProducer<byte[], byte[]> producer;
    KafkaConsumer<byte[], byte[]> consumer;
    String bootstrapServers;
    String groupId;
    String topic;
    long offset;
    boolean isAsyncWrite;

    public boolean isAsyncWrite() {
        return isAsyncWrite;
    }

    public void setAsyncWrite(boolean asyncWrite) {
        isAsyncWrite = asyncWrite;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void init(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers",bootstrapServers);
        properties.put("group.id",groupId);
        properties.put("enable.auto.commit","false");
        properties.put("key.deserializer","");
        ByteArrayDeserializer deserializer = new ByteArrayDeserializer();
        consumer = new KafkaConsumer<>(properties,deserializer,deserializer);
        consumer.subscribe(Arrays.asList(topic));

        //producer
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 30000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        producer = new KafkaProducer<>(props);
    }

    @Override
    public long writeLog(byte[] key, byte[] value) throws ExecutionException, InterruptedException {
        ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>(topic, key, value);
        if(isAsyncWrite==false) {
            offset = producer.send(record).get().offset();
            return offset;
        }
        else{
            producer.send(record,this);
            return -1;
        }
    }

    @Override
    public List<LogRecord> poll(Duration timeout) {

        List<LogRecord> logRecs = new ArrayList<>();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(30);
        for (ConsumerRecord<byte[], byte[]> record : records) {
//            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            LogRecord r = new LogRecord();
            r.setKey(record.key());
            r.setOffset(record.offset());
            r.setValue(record.value());
            r.setTimestamp(record.timestamp());
            logRecs.add(r);
        }
        return logRecs;
    }

    @Override
    public void seek(int partition, long offset) {
        consumer.seek(new TopicPartition(topic,partition),offset);
    }

    @Override
    public long getLastOffset() {
        List<TopicPartition> partitions = new ArrayList<>();
        TopicPartition actualTopicPartition = new TopicPartition(topic, 0);
        partitions.add(actualTopicPartition);
        Long actualEndOffset = this.consumer.endOffsets(partitions).get(actualTopicPartition);
//        long actualPosition = consumer.position(actualTopicPartition);
       return actualEndOffset;

    }


    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        offset = recordMetadata.offset();
    }
}

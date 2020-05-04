/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo.kafka.controller.response;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A key/value pair to be received from Kafka. This also consists of a topic name and
 * a partition number from which the record is being received, an offset that points
 * to the record in a Kafka partition, and a timestamp as marked by the corresponding ProducerRecord.
 */
@Data
public class LocalConsumerRecord<K, V> implements Serializable {

    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private TimestampType timestampType;
    private int serializedKeySize;
    private int serializedValueSize;
    private RecordHeaders headers;
    private K key;
    private V value;

    private volatile Long checksum;

    public static <K, V> List<LocalConsumerRecord<K, V>> change(List<ConsumerRecord<K, V>> consumerRecords) {
        List<LocalConsumerRecord<K, V>> records = new ArrayList<>();
        consumerRecords.forEach(vo -> {
            LocalConsumerRecord<K, V> localConsumerRecord =
                    new LocalConsumerRecord<K, V>(
                            vo.topic(),
                            vo.partition(),
                            vo.offset(),
                            vo.timestamp(),
                            vo.timestampType(),
                            vo.serializedKeySize(),
                            vo.serializedValueSize(),
                            RecordHeaders.change(vo.headers()),
                            vo.key(),
                            vo.value());
            records.add(localConsumerRecord);

        });
        return records;
    }

    public LocalConsumerRecord(String topic,
                               int partition,
                               long offset,
                               long timestamp,
                               TimestampType timestampType,
                               int serializedKeySize,
                               int serializedValueSize,
                               RecordHeaders headers,
                               K key,
                               V value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.headers = headers;
        this.key = key;
        this.value = value;
        this.checksum = checksum;
    }
}

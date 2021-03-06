/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.connector.kafka;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * See {@link com.hazelcast.jet.processor.KafkaProcessors#writeKafka(String,
 * Properties)}.
 */
public final class WriteKafkaP<K, V> extends AbstractProcessor {

    private final String topic;
    private final KafkaProducer<K, V> producer;

    WriteKafkaP(String topic, KafkaProducer<K, V> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        Map.Entry<K, V> entry = (Map.Entry<K, V>) item;
        producer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue()));
        return true;
    }

    @Override
    public boolean complete() {
        producer.flush();
        return true;
    }

    public static class Supplier<K, V> implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String topicId;
        private final Properties properties;

        private transient KafkaProducer<K, V> producer;

        public Supplier(String topicId, Properties properties) {
            this.topicId = topicId;
            this.properties = properties;
        }

        @Override
        public void init(@Nonnull Context context) {
            producer = new KafkaProducer<>(properties);
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return Stream.generate(() -> new WriteKafkaP<>(topicId, producer))
                         .limit(count)
                         .collect(toList());
        }

        @Override
        public void complete(Throwable error) {
            producer.close();
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.kafka.writer;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.KafkaTestBase;
import org.apache.gobblin.kafka.schemareg.ConfigDrivenMd5SchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.kafka.schemareg.SchemaRegistryException;
import org.apache.gobblin.kafka.serialize.LiAvroDeserializer;
import org.apache.gobblin.kafka.serialize.LiAvroSerializer;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.writer.WriteCallback;
import org.apache.gobblin.writer.WriteResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@Slf4j
public class Kafka32DataWriterTest {


  private final KafkaTestBase _kafkaTestHelper;
  public Kafka32DataWriterTest()
      throws InterruptedException, RuntimeException {
    _kafkaTestHelper = new KafkaTestBase();
  }

  @BeforeSuite(alwaysRun = true)
  public void beforeSuite() {
    log.warn("Process id = " + ManagementFactory.getRuntimeMXBean().getName());

    _kafkaTestHelper.startServers();
  }

  @AfterSuite(alwaysRun = true)
  public void afterSuite()
      throws IOException {
    try {
      _kafkaTestHelper.stopClients();
    } finally {
      _kafkaTestHelper.stopServers();
    }
  }

  @Test
  public void testStringSerialization()
      throws IOException, InterruptedException, ExecutionException {
    String topic = "testStringSerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "127.0.0.1:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Kafka32DataWriter<String, String> kafka32DataWriter = new Kafka32DataWriter<>(props);
    String messageString = "foobar";
    WriteCallback callback = mock(WriteCallback.class);
    Future<WriteResponse> future;

    try {
      future = kafka32DataWriter.write(messageString, callback);
      kafka32DataWriter.flush();
      verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
      verify(callback, never()).onFailure(isA(Exception.class));
      Assert.assertTrue(future.isDone(), "Future should be done");
      System.out.println(future.get().getStringResponse());
      byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().value();
      String messageReceived = new String(message);
      Assert.assertEquals(messageReceived, messageString);
    } finally {
      kafka32DataWriter.close();
    }


  }

  @Test
  public void testBinarySerialization()
      throws IOException, InterruptedException {
    String topic = "testBinarySerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"bootstrap.servers", "127.0.0.1:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX+"value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    Kafka32DataWriter<String, byte[]> kafka32DataWriter = new Kafka32DataWriter<>(props);
    WriteCallback callback = mock(WriteCallback.class);
    byte[] messageBytes = TestUtils.generateRandomBytes();

    try {
      kafka32DataWriter.write(messageBytes, callback);
    } finally {
      kafka32DataWriter.close();
    }

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));
    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().value();
    Assert.assertEquals(message, messageBytes);
  }

  @Test
  public void testAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization08";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers",
        "127.0.0.1:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        LiAvroSerializer.class.getName());

    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
        + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    Kafka32DataWriter<String, GenericRecord> kafka32DataWriter = new Kafka32DataWriter<>(props);
    WriteCallback callback = mock(WriteCallback.class);

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    try {
      kafka32DataWriter.write(record, callback);
    } finally {
      kafka32DataWriter.close();
    }

    log.info("Kafka events written");

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));

    byte[] message = _kafkaTestHelper.getIteratorForTopic(topic).next().value();

    log.info("Kafka events read, start to check result... ");
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    Assert.assertEquals(record.toString(), receivedRecord.toString());
  }


  @Test
  public void testKeyedAvroSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization32";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers",
        "127.0.0.1:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
        LiAvroSerializer.class.getName());
    props.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYED_CONFIG, "true");
    String keyField = "field1";
    props.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYFIELD_CONFIG, keyField);


    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
            + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    Kafka32DataWriter<String, GenericRecord> kafka32DataWriter = new Kafka32DataWriter<>(props);
    WriteCallback callback = mock(WriteCallback.class);

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    try {
      kafka32DataWriter.write(record, callback);
    } finally {
      kafka32DataWriter.close();
    }

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));
    ConsumerRecord<byte[], byte[]> cRecord = _kafkaTestHelper.getIteratorForTopic(topic).next();
    byte[] key = cRecord.key();
    byte[] message = cRecord.value();
    ConfigDrivenMd5SchemaRegistry schemaReg = new ConfigDrivenMd5SchemaRegistry(topic, record.getSchema());
    LiAvroDeserializer deser = new LiAvroDeserializer(schemaReg);
    GenericRecord receivedRecord = deser.deserialize(topic, message);
    Assert.assertEquals(record.toString(), receivedRecord.toString());
    Assert.assertEquals(new String(key), record.get(keyField));
  }

  @Test
  public void testValueSerialization()
      throws IOException, InterruptedException, SchemaRegistryException {
    String topic = "testAvroSerialization32";
    _kafkaTestHelper.provisionTopic(topic);
    Properties props = new Properties();
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_TOPIC, topic);
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "bootstrap.servers",
        "127.0.0.1:" + _kafkaTestHelper.getKafkaServerPort());
    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX + "value.serializer",
    "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYED_CONFIG, "true");
    String keyField = "field1";
    props.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_KEYFIELD_CONFIG, keyField);
    props.setProperty(KafkaWriterConfigurationKeys.WRITER_KAFKA_VALUEFIELD_CONFIG, keyField);


    // set up mock schema registry

    props.setProperty(KafkaWriterConfigurationKeys.KAFKA_PRODUCER_CONFIG_PREFIX
            + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS,
        ConfigDrivenMd5SchemaRegistry.class.getCanonicalName());

    Kafka32DataWriter<String, GenericRecord> kafka32DataWriter = new Kafka32DataWriter<>(props);
    WriteCallback callback = mock(WriteCallback.class);

    GenericRecord record = TestUtils.generateRandomAvroRecord();
    try {
      kafka32DataWriter.write(record, callback);
    } finally {
      kafka32DataWriter.close();
    }

    verify(callback, times(1)).onSuccess(isA(WriteResponse.class));
    verify(callback, never()).onFailure(isA(Exception.class));
    ConsumerRecord<byte[], byte[]> cRecord = _kafkaTestHelper.getIteratorForTopic(topic).next();
    byte[] key = cRecord.key();
    byte[] message = cRecord.value();
    Assert.assertEquals(new String(message), record.get(keyField));
    Assert.assertEquals(new String(key), record.get(keyField));
  }

}

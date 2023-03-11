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

package org.apache.gobblin.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.gobblin.test.TestUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Time;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.zk.EmbeddedZookeeper;
import lombok.extern.slf4j.Slf4j;


/**
 * A private class for starting a suite of servers for Kafka
 * Calls to start and shutdown are reference counted, so that the suite is started and shutdown in pairs.
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 */
@Slf4j
class KafkaServerSuite {

  static KafkaServerSuite _instance;
  static KafkaServerSuite getInstance()
  {
    if (null == _instance)
    {
      _instance = new KafkaServerSuite();
      return _instance;
    }
    else
    {
      return _instance;
    }
  }

  private int _brokerId = 0;
  private EmbeddedZookeeper _zkServer;
  private AdminClient _adminClient;
  private KafkaServer _kafkaServer;
  private final int _kafkaServerPort;
  private String _bootstrapServersString;
  private final AtomicInteger _numStarted;


  public AdminClient getAdminClient() {
		return _adminClient;
  }

  public KafkaServer getKafkaServer() {
    return _kafkaServer;
  }

  public int getKafkaServerPort() {
    return _kafkaServerPort;
  }

  public String getBootstrapServersString() {
    return _bootstrapServersString;
  }

  private KafkaServerSuite()
  {
    _kafkaServerPort = TestUtils.findFreePort();
    _bootstrapServersString = "UNINITIALIZED_HOST_PORT";
    _numStarted = new AtomicInteger(0);
  }


  void start()
    throws RuntimeException {
    if (_numStarted.incrementAndGet() == 1) {
      _zkServer = new EmbeddedZookeeper();
      String _local_zkConnectString = "127.0.0.1:"+_zkServer.port();
      log.warn("Starting up Kafka server suite. Zk at " + _local_zkConnectString + "; Kafka server at "
				+ _kafkaServerPort);
//      _zkClient = new ZkClient(_zkConnectString, 30000, 30000, ZKStringSerializer$.MODULE$);
      _bootstrapServersString="localhost:" + _kafkaServerPort;
      Properties props = kafka.utils.TestUtils.createBrokerConfig(
          _brokerId,
          _local_zkConnectString,
          kafka.utils.TestUtils.createBrokerConfig$default$3(),
          kafka.utils.TestUtils.createBrokerConfig$default$4(),
          _kafkaServerPort,
          kafka.utils.TestUtils.createBrokerConfig$default$6(),
          kafka.utils.TestUtils.createBrokerConfig$default$7(),
          kafka.utils.TestUtils.createBrokerConfig$default$8(),
          kafka.utils.TestUtils.createBrokerConfig$default$9(),
          kafka.utils.TestUtils.createBrokerConfig$default$10(),
          kafka.utils.TestUtils.createBrokerConfig$default$11(),
          kafka.utils.TestUtils.createBrokerConfig$default$12(),
          kafka.utils.TestUtils.createBrokerConfig$default$13(),
          kafka.utils.TestUtils.createBrokerConfig$default$14(),
          kafka.utils.TestUtils.createBrokerConfig$default$15(),
          kafka.utils.TestUtils.createBrokerConfig$default$16(),
          kafka.utils.TestUtils.createBrokerConfig$default$17(),
          kafka.utils.TestUtils.createBrokerConfig$default$18(),
          kafka.utils.TestUtils.createBrokerConfig$default$19(),
          kafka.utils.TestUtils.createBrokerConfig$default$20());


      KafkaConfig config = new KafkaConfig(props);
//      Time mock = new MockTime();
      _kafkaServer = kafka.utils.TestUtils.createServer(config, Time.SYSTEM);
      Properties adminPr=new Properties();
      adminPr.put("bootstrap.servers", _bootstrapServersString);
      _adminClient = AdminClient.create(adminPr);
    }
    else
    {
      log.info("Kafka server suite already started... continuing");
    }
  }

  CreateTopicsResult createTopic(String name, int numPartitions, short replicationFactor) {
	  NewTopic nTopic=new NewTopic( name,  numPartitions,  replicationFactor);
	 return _adminClient.createTopics(Collections.singletonList(nTopic));
  }
  void shutdown() {
    if (_numStarted.decrementAndGet() == 0) {
      log.info("Shutting down Kafka server suite");
      _adminClient.close();
      _kafkaServer.shutdown();
      _adminClient.close();
      _zkServer.shutdown();
    }
    else {
      log.info("Kafka server suite still in use ... not shutting down yet");
    }
  }

}
@Slf4j
class KafkaConsumerSuite {
  private final KafkaConsumer<byte[], byte[]> _consumer;
  private final AdminClient _adminClient;
//  private final ConsumerRecords<byte[], byte[]> _records;
//  private final ConsumerIterator<byte[], byte[]> _iterator;
  
  private final String _topic;

  KafkaConsumerSuite(String bootstrapServers, String topic)
  {
    _topic = topic;
    Properties consumeProps = new Properties();
    consumeProps.put("bootstrap.servers", bootstrapServers);
//  consumeProps.put("zookeeper.connect", zkConnectString);
	consumeProps.put("group.id", _topic+"-"+System.nanoTime());
    consumeProps.put("zookeeper.session.timeout.ms", "10000");
    consumeProps.put("zookeeper.sync.time.ms", "10000");
    consumeProps.put("auto.commit.interval.ms", "10000");
    consumeProps.put("_consumer.timeout.ms", "10000");
    consumeProps.put("auto.offset.reset", "earliest");
	consumeProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	consumeProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    
    _consumer = new KafkaConsumer<byte[],byte[]>(consumeProps);
    _consumer.subscribe(Collections.singleton(topic));
    _adminClient =  AdminClient.create(consumeProps);
//    _records=_consumer.poll(1000l);
    /*
     Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        _consumer.createMessageStreams(ImmutableMap.of(this._topic, 1));
    
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this._topic);
    _stream = streams.get(0);
    _records = _stream.iterator();
     */
 
  }

  void shutdown()
  {
    _consumer.close();
  }
  void shutdownClients() {
		_adminClient.close();
	}
  DeleteTopicsResult deleteTopic(String topicName) {
	  topicName=(topicName==null || topicName=="")? _topic:topicName;
	  return _adminClient.deleteTopics(Collections.singleton(topicName));
  }



	public ConsumerRecords<byte[], byte[]> getConsumerRecords() {
		return  _consumer.poll(10000);
	}
	public Iterator<ConsumerRecord<byte[], byte[]>> getIteratorForTopic() {
		    return getConsumerRecords().iterator();
	}
}

/**
 * A Helper class for testing against Kafka
 * A suite of servers (Zk, Kafka etc) will be started just once per process
 * Consumer and iterator will be created per instantiation and is one instance per topic.
 */
@Slf4j
public class KafkaTestBase implements Closeable {

  private final KafkaServerSuite _kafkaServerSuite;
  private final Map<String, KafkaConsumerSuite> _topicConsumerMap;

  public KafkaTestBase() throws InterruptedException, RuntimeException {

    this._kafkaServerSuite = KafkaServerSuite.getInstance();
    this._topicConsumerMap = new HashMap<>();
  }

  public synchronized void startServers()
  {
    _kafkaServerSuite.start();
  }

  public void stopServers()
  {
      _kafkaServerSuite.shutdown();
  }

  public void start() {
    startServers();
  }

  public void stopClients() throws IOException {
    for (Map.Entry<String, KafkaConsumerSuite> consumerSuiteEntry: _topicConsumerMap.entrySet())
    {
      consumerSuiteEntry.getValue().shutdown();
      DeleteTopicsResult deleteTResult=consumerSuiteEntry.getValue().deleteTopic(consumerSuiteEntry.getKey());
      log.info("deleteTResult {}",deleteTResult);
//      AdminUtils.deleteTopic(ZkUtils.apply(_kafkaServerSuite.getZkClient(), false),
//          consumerSuiteEntry.getKey());
    }
  }

  @Override
  public void close() throws IOException {
    stopClients();
    stopServers();
  }

  public void provisionTopic(String topic) {
    if (_topicConsumerMap.containsKey(topic)) {
      // nothing to do: return
    } else {
      // provision topic
//      AdminUtils.createTopic(ZkUtils.apply(_kafkaServerSuite.getZkClient(), false),
//          topic, 1, 1, new Properties(), null);
      CreateTopicsResult createTResult=_kafkaServerSuite.createTopic(topic, 1, (short)1);
      log.info("CreateTopicsResult {}",createTResult);
      List<KafkaServer> servers = new ArrayList<>();
      servers.add(_kafkaServerSuite.getKafkaServer());
      kafka.utils.TestUtils.waitForPartitionMetadata(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
//      kafka.utils.TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
      KafkaConsumerSuite consumerSuite = new KafkaConsumerSuite(_kafkaServerSuite.getBootstrapServersString(), topic);
      _topicConsumerMap.put(topic, consumerSuite);
    }
  }


  public Iterator<ConsumerRecord<byte[], byte[]>> getIteratorForTopic(String topic) {
	    return getConsumerRecordsForTopic(topic).iterator();
	  }
  
  private ConsumerRecords<byte[], byte[]> getConsumerRecordsForTopic(String topic) {
    if (_topicConsumerMap.containsKey(topic))
    {
      return _topicConsumerMap.get(topic).getConsumerRecords();
    }
    else
    {
      throw new IllegalStateException("Could not find provisioned topic" + topic + ": call provisionTopic before");
    }
  }

  public int getKafkaServerPort() {
    return _kafkaServerSuite.getKafkaServerPort();
  }

  public String getBootstrapServersString() {
    return this._kafkaServerSuite.getBootstrapServersString();
  }
}


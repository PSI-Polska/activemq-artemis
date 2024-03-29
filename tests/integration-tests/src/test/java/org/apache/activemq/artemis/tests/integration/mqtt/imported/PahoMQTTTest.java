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
 */

package org.apache.activemq.artemis.tests.integration.mqtt.imported;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.artemis.tests.util.RandomUtil;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.jboss.logging.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class PahoMQTTTest extends MQTTTestSupport {

   private static final Logger log = Logger.getLogger(PahoMQTTTest.class);

   @Parameterized.Parameters(name = "protocol={0}")
   public static Collection<Object[]> getParams() {
      return Arrays.asList(new Object[][] {{"tcp"}, {"ws"}});
   }

   public String protocol;

   public PahoMQTTTest(String protocol) {
      this.protocol = protocol;
   }

   @Test(timeout = 300000)
   public void testLotsOfClients() throws Exception {

      final int CLIENTS = Integer.getInteger("PahoMQTTTest.CLIENTS", 100);
      log.debug("Using: {} clients: " + CLIENTS);

      final AtomicInteger receiveCounter = new AtomicInteger();
      MqttClient client = createPahoClient("consumer");
      client.setCallback(new MqttCallback() {
         @Override
         public void connectionLost(Throwable cause) {
         }

         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            receiveCounter.incrementAndGet();
         }

         @Override
         public void deliveryComplete(IMqttDeliveryToken token) {
         }
      });
      client.connect();
      client.subscribe("test");

      final AtomicReference<Throwable> asyncError = new AtomicReference<>();
      final CountDownLatch connectedDoneLatch = new CountDownLatch(CLIENTS);
      final CountDownLatch disconnectDoneLatch = new CountDownLatch(CLIENTS);
      final CountDownLatch sendBarrier = new CountDownLatch(1);

      for (int i = 0; i < CLIENTS; i++) {
         Thread.sleep(10);
         new Thread(null, null, "client:" + i) {
            @Override
            public void run() {
               try {
                  MqttClient client = createPahoClient(Thread.currentThread().getName());
                  client.connect();
                  connectedDoneLatch.countDown();
                  sendBarrier.await();
                  for (int i = 0; i < 10; i++) {
                     Thread.sleep(1000);
                     client.publish("test", "hello".getBytes(), 1, false);
                  }
                  client.disconnect();
                  client.close();
               } catch (Throwable e) {
                  e.printStackTrace();
                  asyncError.set(e);
               } finally {
                  disconnectDoneLatch.countDown();
               }
            }
         }.start();
      }

      connectedDoneLatch.await();
      assertNull("Async error: " + asyncError.get(), asyncError.get());
      sendBarrier.countDown();

      log.debug("All clients connected... waiting to receive sent messages...");

      // We should eventually get all the messages.
      within(30, TimeUnit.SECONDS, new Task() {
         @Override
         public void run() throws Exception {
            assertTrue(receiveCounter.get() == CLIENTS * 10);
         }
      });

      log.debug("All messages received.");

      disconnectDoneLatch.await();
      assertNull("Async error: " + asyncError.get(), asyncError.get());
   }

   @Test(timeout = 300000)
   public void testSendAndReceiveMQTT() throws Exception {
      final CountDownLatch latch = new CountDownLatch(1);

      MqttClient consumer = createPahoClient("consumerId");
      MqttClient producer = createPahoClient("producerId");

      consumer.connect();
      consumer.subscribe("test");
      consumer.setCallback(new MqttCallback() {
         @Override
         public void connectionLost(Throwable cause) {

         }

         @Override
         public void messageArrived(String topic, MqttMessage message) throws Exception {
            latch.countDown();
         }

         @Override
         public void deliveryComplete(IMqttDeliveryToken token) {

         }
      });

      producer.connect();
      producer.publish("test", "hello".getBytes(), 1, false);

      waitForLatch(latch);
      producer.disconnect();
      producer.close();
   }

   @Test(timeout = 300000)
   public void testSessionPresentWithCleanSession() throws Exception {
      MqttClient client = createPahoClient(RandomUtil.randomString());
      MqttConnectOptions options = new MqttConnectOptions();
      options.setCleanSession(true);
      IMqttToken result = client.connectWithResult(options);
      assertFalse(result.getSessionPresent());
      client.disconnect();
   }

   @Test(timeout = 300000)
   public void testSessionPresent() throws Exception {
      MqttClient client = createPahoClient(RandomUtil.randomString());
      MqttConnectOptions options = new MqttConnectOptions();
      options.setCleanSession(false);
      IMqttToken result = client.connectWithResult(options);
      assertFalse(result.getSessionPresent());
      client.disconnect();
      result = client.connectWithResult(options);
      assertTrue(result.getSessionPresent());
   }

   private MqttClient createPahoClient(String clientId) throws MqttException {
      return new MqttClient(protocol + "://localhost:" + getPort(), clientId, new MemoryPersistence());
   }

}

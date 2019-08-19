/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.rest.push.balancer;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.rest.queue.QueueServiceManager;
import org.apache.activemq.artemis.rest.queue.push.PushStore;
import org.apache.activemq.artemis.rest.topic.TopicPushStore;
import org.apache.activemq.artemis.rest.topic.TopicServiceManager;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.activemq.artemis.api.core.ActiveMQAddressExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import static org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID;

import static org.apache.activemq.artemis.rest.ActiveMQRestLogger.LOGGER;

public class PushBalancer {

    private static final String TOPIC_PREFIX = "org.apache.activemq.artemis.rest.push";
    private static final String INSTANCE_ID_PROPERTY = "instanceId";

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final SimpleString topic;
    private final long interval;

    private ClientSessionFactory sessionFactory;
    private String instanceId;
    private QueueServiceManager queueServiceManager;
    private TopicServiceManager topicServiceManager;

    private Marshaller marshaller;
    private ClientSession session;
    private ClientConsumer consumer;
    private ClientProducer producer;

    public PushBalancer(String topicSuffix, long interval) {
        this.topic = new SimpleString(TOPIC_PREFIX + "." + topicSuffix);
        this.interval = interval;
    }

    public PushBalancer() {
        this("balancer", TimeUnit.MINUTES.toMillis(5L));
    }

    public void setSessionFactory(ClientSessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setQueueServiceManager(QueueServiceManager queueServiceManager) {
        this.queueServiceManager = queueServiceManager;
    }

    public void setTopicServiceManager(TopicServiceManager topicServiceManager) {
        this.topicServiceManager = topicServiceManager;
    }

    public void start() throws ActiveMQException, JAXBException {
        JAXBContext ctx = JAXBContext.newInstance( PushInfo.class );
        marshaller = ctx.createMarshaller();

        session = sessionFactory.createSession(true, true);

        createMulticastDestination(session, topic);

        SimpleString filter = new SimpleString(String.format("NOT (%s = '%s')", INSTANCE_ID_PROPERTY, instanceId));
        consumer = session.createConsumer(topic, filter);
        consumer.setMessageHandler(new PushInfoMessageHandler(queueServiceManager, topicServiceManager));

        producer = session.createProducer(topic);

        session.start();

        executorService.scheduleAtFixedRate(this::sendInfo, interval, interval, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        try {
            executorService.shutdown();
            if (!producer.isClosed())
                producer.close();
            if (!consumer.isClosed())
                consumer.close();
            if (!session.isClosed()) {
                session.stop();
                session.close();
            }
        } catch (ActiveMQException ex) {
            LOGGER.error("Unable to stop session", ex);
        }
    }
    
    private void createMulticastDestination(final ClientSession session,SimpleString topic) throws ActiveMQException
    {   
        try
        {
            session.createTemporaryQueue(topic, RoutingType.MULTICAST, topic);
        }
        catch (ActiveMQQueueExistsException | ActiveMQAddressExistsException ex)
        {
            //do nothing - it is better to discard the exception than check for queue existence as it poses less risk for race conditions
        }
    }

    private void sendInfo()
    {
        try {
            PushStore pushStore = queueServiceManager.getPushStore();
            TopicPushStore topicPushStore = topicServiceManager.getPushStore();

            long pushStoreCount = pushStore.count();
            long topicPushStoreCount = topicPushStore.count();
            PushInfo info = new PushInfo(instanceId, pushStoreCount, topicPushStoreCount);
            String body = write(info);

            ClientMessage message = session.createMessage(Message.TEXT_TYPE, false);
            message.putStringProperty(INSTANCE_ID_PROPERTY, instanceId);
            message.putStringProperty(HDR_DUPLICATE_DETECTION_ID, UUID.randomUUID().toString());
            message.getBodyBuffer().writeUTF(body);

            producer.send(message);
        } catch (Exception ex) {
            LOGGER.warn("Sending info failed", ex);
        }
    }

    private String write(PushInfo pushInfo) throws JAXBException {
        StringWriter writer = new StringWriter();
        marshaller.marshal(pushInfo, writer);

        return writer.toString();
    }
}

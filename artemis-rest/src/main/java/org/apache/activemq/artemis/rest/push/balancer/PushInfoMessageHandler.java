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

import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.MessageHandler;
import org.apache.activemq.artemis.rest.queue.QueueResource;
import org.apache.activemq.artemis.rest.queue.QueueServiceManager;
import org.apache.activemq.artemis.rest.queue.push.DestinationPushStore;
import org.apache.activemq.artemis.rest.queue.push.PushConsumer;
import org.apache.activemq.artemis.rest.queue.push.PushConsumerResource;
import org.apache.activemq.artemis.rest.queue.push.PushStore;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.topic.*;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import java.io.StringReader;
import java.util.*;

import static org.apache.activemq.artemis.rest.ActiveMQRestLogger.LOGGER;

class PushInfoMessageHandler implements MessageHandler {

    private final Unmarshaller unmarshaller;
    private final QueueServiceManager queueServiceManager;
    private final TopicServiceManager topicServiceManager;

    PushInfoMessageHandler(QueueServiceManager queueServiceManager,
                           TopicServiceManager topicServiceManager) throws JAXBException {
        JAXBContext ctx = JAXBContext.newInstance(PushInfo.class);
        unmarshaller = ctx.createUnmarshaller();

        this.queueServiceManager = queueServiceManager;
        this.topicServiceManager = topicServiceManager;
    }

    @Override
    public void onMessage(ClientMessage message) {
        try {
            String body = message.getBodyBuffer().readUTF();
            PushInfo info = read(body);

            LOGGER.info(String.format(
                    "Instance %s: queues = %d, topics = %d",
                    info.getInstanceId(),
                    info.getPushConsumers(),
                    info.getPushSubscriptions()));

            rebalancePushRegistrations(info.getPushConsumers());
            rebalanceTopicPushRegistrations(info.getPushSubscriptions());
        } catch (Exception ex) {
            LOGGER.error("Could not process message", ex);
        }
    }

    private PushInfo read(String value) throws JAXBException {
        StringReader reader = new StringReader(value);
        return (PushInfo) unmarshaller.unmarshal(reader);
    }

    private void rebalancePushRegistrations(long count) throws Exception {
        PushStore pushStore = queueServiceManager.getPushStore();
        if (pushStore instanceof DestinationPushStore) {
            long halfOfInstanceRegistrations = pushStore.count() / 2L;
            if (count < halfOfInstanceRegistrations) {
                long counter = 0L;

                Map<String, QueueResource> queueResources = queueServiceManager.getDestination().getQueues();
                Iterator<String> queueResourcesKeyIterator = queueResources.keySet().iterator();
                while (queueResourcesKeyIterator.hasNext()) {
                    String queueResourceKey = queueResourcesKeyIterator.next();
                    QueueResource queueResource = queueResources.get(queueResourceKey);
                    if (queueResource != null) {
                        PushConsumerResource pushConsumerResource = queueResource.getPushConsumers();
                        Map<String, PushConsumer> pushConsumers = pushConsumerResource.getConsumers();
                        Iterator<String> pushConsumerKeyIterator = pushConsumers.keySet().iterator();
                        while (pushConsumerKeyIterator.hasNext()) {
                            String pushConsumerKey = pushConsumerKeyIterator.next();
                            PushConsumer pushConsumer = pushConsumers.remove(pushConsumerKey);
                            if (pushConsumer != null) {
                                pushConsumer.stop();

                                try {
                                    PushRegistration registration = pushConsumer.getRegistration();
                                    pushStore.remove(registration);
                                    ((DestinationPushStore) pushStore).addGlobally(registration);

                                    counter++;
                                    if (counter == halfOfInstanceRegistrations) {
                                        return;
                                    }
                                } catch (Exception ex) {
                                    pushConsumerResource.addRegistration(pushConsumer.getRegistration());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void rebalanceTopicPushRegistrations(long count) throws Exception {
        TopicPushStore topicPushStore = topicServiceManager.getPushStore();
        if (topicPushStore instanceof DestinationPushStore) {
            long halfOfInstanceRegistrations = topicPushStore.count() / 2L;
            if (count < halfOfInstanceRegistrations) {
                long counter = 0L;

                Map<String, TopicResource> topicResources = topicServiceManager.getDestination().getTopics();
                Iterator<String> topicResourcesKeyIterator = topicResources.keySet().iterator();
                while (topicResourcesKeyIterator.hasNext()) {
                    String topicResourceKey = topicResourcesKeyIterator.next();
                    TopicResource topicResource = topicResources.get(topicResourceKey);
                    if (topicResource != null) {
                        PushSubscriptionsResource pushSubscriptionResource = topicResource.getPushSubscriptions();
                        Map<String, PushSubscription> pushSubscriptions = pushSubscriptionResource.getConsumers();
                        Iterator<String> pushSubscriptionKeyIterator = pushSubscriptions.keySet().iterator();
                        while (pushSubscriptionKeyIterator.hasNext()) {
                            String pushSubscriptionKey = pushSubscriptionKeyIterator.next();
                            PushSubscription pushSubscription = pushSubscriptions.remove(pushSubscriptionKey);
                            if (pushSubscription != null) {
                                pushSubscription.stop();
                                pushSubscriptionResource.deleteSubscriberQueue(pushSubscription);

                                try {
                                    PushTopicRegistration registration = (PushTopicRegistration) pushSubscription.getRegistration();
                                    topicPushStore.remove(registration);
                                    ((DestinationPushStore) topicPushStore).addGlobally(registration);

                                    counter++;
                                    if (counter == halfOfInstanceRegistrations) {
                                        return;
                                    }
                                } catch (Exception ex) {
                                    LOGGER.warn("Could not add registration to the global queue", ex);
                                    pushSubscriptionResource.addRegistration((PushTopicRegistration) pushSubscription.getRegistration());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

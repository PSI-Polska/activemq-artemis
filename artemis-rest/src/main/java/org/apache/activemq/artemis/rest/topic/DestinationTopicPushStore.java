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
package org.apache.activemq.artemis.rest.topic;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.queue.push.DestinationPushStore;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistrationUnmarshaler;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.List;

class DestinationTopicPushStore extends DestinationPushStore implements TopicPushStore {
    
    private static final String TOPIC_ID = "reg_topic";

    protected DestinationTopicPushStore( ClientSessionFactory factory,
                                         PushRegistrationUnmarshaler unmarshaler, String IdOfInstance,
                                         String globalQueueName ) throws JAXBException, ActiveMQException
    {
        super( factory, unmarshaler, IdOfInstance, globalQueueName );
    }


    @Override
    public List<PushRegistration> getRegistrations() {
        throw new UnsupportedOperationException("Not applicable at topic level");
    }

    @Override
    public List<PushTopicRegistration> getTopicRegistrations() throws ActiveMQException, JAXBException {
        try (ClientSession session = factory.createSession(false,false);
             ClientConsumer consumer = session.createConsumer( privateQueueName, true)) {
            session.start();
            List<PushTopicRegistration> toReturn = new ArrayList<>();
            ClientMessage message = consumer.receiveImmediate();
            while(message != null){
                toReturn.add(unmarshallPushTopicRegistration(message.getBodyBuffer().readUTF()));
                message = consumer.receiveImmediate();
            }
            session.commit();
            return toReturn;
        }
    }

    @Override
   public List<PushTopicRegistration> getByTopic(String topic) throws ActiveMQException, JAXBException {
      try (ClientSession session = factory.createSession(false,false);
           ClientConsumer consumer = session.createConsumer( privateQueueName, String.format( "%s = '%s'", TOPIC_ID, topic), true)) {
            session.start();
            List<PushTopicRegistration> toReturn = new ArrayList<>();
            ClientMessage message = consumer.receiveImmediate();
            while(message != null){
                toReturn.add(unmarshallPushTopicRegistration(message.getBodyBuffer().readUTF()));
                message = consumer.receiveImmediate();
            }
            session.commit();
            return toReturn;
        }
   }

    @Override
    protected ClientMessage prepareMessageWithPushRegistration(
                                                               ClientSession session,
                                                               PushRegistration reg) throws
                                                                                            JAXBException
    {
        ClientMessage messageToSend = super.prepareMessageWithPushRegistration(session, reg);
        if(reg instanceof PushTopicRegistration){
            messageToSend.putStringProperty(TOPIC_ID, ((PushTopicRegistration) reg).getTopic());
        }
        return messageToSend;
    }

    private PushTopicRegistration unmarshallPushTopicRegistration(String msg) throws JAXBException
    {
        return (PushTopicRegistration) unmarshaler.unmarshallPushRegistration( msg );
    }
   
   
}

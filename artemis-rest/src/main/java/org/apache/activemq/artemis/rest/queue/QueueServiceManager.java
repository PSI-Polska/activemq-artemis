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
package org.apache.activemq.artemis.rest.queue;

import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.DestinationPushStore;
import org.apache.activemq.artemis.rest.queue.push.FilePushStore;
import org.apache.activemq.artemis.rest.queue.push.PushStore;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueueServiceManager extends DestinationServiceManager {

   protected PushStore pushStore;
   protected List<QueueDeployment> queues = new ArrayList<>();
   protected QueueDestinationsResource destination;

   public QueueServiceManager(ConnectionFactoryOptions jmsOptions)
   {
      super(jmsOptions, "queue_storage");
   }

   public List<QueueDeployment> getQueues() {
      return queues;
   }

   public void setQueues(List<QueueDeployment> queues) {
      this.queues = queues;
   }

   public PushStore getPushStore() {
      return pushStore;
   }

   public void setPushStore(PushStore pushStore) {
      this.pushStore = pushStore;
   }

   public QueueDestinationsResource getDestination() {
      return destination;
   }

   public void setDestination(QueueDestinationsResource destination) {
      this.destination = destination;
   }

   @Override
   public void start() throws Exception {
      initDefaults();

      destination = new QueueDestinationsResource(this);

      started = true;

      if (pushStoreQueuePrefix != null && pushStore == null){
          pushStore = new DestinationPushStore(sessionFactory,unmarshaler, getPrivateQueueName(), getGlobalQueueName());
      }
      if (pushStoreFile != null && pushStore == null) {
         pushStore = new FilePushStore(pushStoreFile);
      }

      List<String> registeredQueues = pushStore.getRegistrations().stream().filter( PushRegistration::isEnabled )
          .map( PushRegistration::getDestination ).collect( Collectors.toList() );

      registeredQueues.forEach( name -> this.restartDestinationResource( () -> destination.findQueue( name ) ) );

      for (QueueDeployment queueDeployment : queues) {
         deploy(queueDeployment);
      }

      startGlobalQueueHandler( content -> {
             try
             {
                destination.createInternal( content.getDestination(), content.getSelector(), true );
             }
             catch( Exception aE )
             {
             }
             ActiveMQRestLogger.LOGGER.info("Recreating push subscription for :"+content.getDestination());
             QueueResource res = restartDestinationResource( () ->  destination.findQueue( content.getDestination() ));
             res.getPushConsumers().performCreation( content );
          } );
   }


   public void deploy(QueueDeployment queueDeployment) throws Exception {
      if (!started) {
         throw new Exception("You must start() this class instance before deploying");
      }
      String queueName = queueDeployment.getName();
      try (ClientSession session = sessionFactory.createSession(false, false, false)) {
         ClientSession.AddressQuery query = session.addressQuery(SimpleString.toSimpleString(queueName));
         if (!query.isExists()) {
            session.createAddress(SimpleString.toSimpleString(queueName), RoutingType.ANYCAST, true);
            session.createQueue(SimpleString.toSimpleString(queueName), RoutingType.ANYCAST, SimpleString.toSimpleString(queueName), queueDeployment.isDurableSend());
         } else {
            ClientSession.QueueQuery qquery = session.queueQuery(SimpleString.toSimpleString(queueName));
            if (!qquery.isExists()) {
               session.createQueue(SimpleString.toSimpleString(queueName), RoutingType.ANYCAST, SimpleString.toSimpleString(queueName), queueDeployment.isDurableSend());
            }
         }
      }

      destination.createQueueResource(queueName, queueDeployment.isDurableSend(), queueDeployment.getConsumerSessionTimeoutSeconds(), queueDeployment.isDuplicatesAllowed());

   }

   @Override
   public void stop() {
      if (started == false)
         return;
      for (QueueResource queue : destination.getQueues().values()) {
         queue.stop();
      }
      try {
         timeoutTask.stop();
         sessionFactory.close();
      } catch (Exception e) {
      }
   }
}

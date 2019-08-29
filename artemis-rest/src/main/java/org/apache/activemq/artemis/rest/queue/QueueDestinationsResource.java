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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSession.AddressQuery;
import org.apache.activemq.artemis.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.FileJMSConfiguration;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumerResource;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.util.Constants;
import org.w3c.dom.Document;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Path(Constants.PATH_FOR_QUEUES)
public class QueueDestinationsResource {

   public static final SimpleString UNREGISTRATION_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.push.unregistration");

   private final Map<String, QueueResource> queues = new ConcurrentHashMap<>();
   private final QueueServiceManager manager;

   public QueueDestinationsResource(QueueServiceManager manager) {
      this.manager = manager;
   }

   @POST
   @Consumes("application/activemq.jms.queue+xml")
   public Response createJmsQueue(@Context UriInfo uriInfo, Document document) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      try {
         JMSQueueConfiguration queue = FileJMSConfiguration.parseQueueConfiguration(document.getDocumentElement());
         ActiveMQQueue activeMQQueue = ActiveMQDestination.createQueue(queue.getName());
         String queueName = activeMQQueue.getAddress();
         createInternal( queueName, queue.getSelector(), queue.isDurable() );
         URI uri = uriInfo.getRequestUriBuilder().path(queueName).build();
         return Response.created(uri).build();
      } catch (Exception e) {
         if (e instanceof WebApplicationException)
            throw (WebApplicationException) e;
         throw new WebApplicationException(e, Response.serverError().type("text/plain").entity("Failed to create queue.").build());
      }
   }

   @DELETE
   @Path("/{queue-name}/push-consumers/{consumer-id}")
   public void deleteConsumer(@Context UriInfo uriInfo,
                              @PathParam("queue-name") String name,
                              @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      ClientSessionFactory sessionFactory = manager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(true, true);
           ClientProducer clientProducer = clientSession.createProducer(UNREGISTRATION_QUEUE_NAME)) {
         AddressQuery addressQuery = clientSession.addressQuery(UNREGISTRATION_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         QueueQuery queueQuery = clientSession.queueQuery(UNREGISTRATION_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, UNREGISTRATION_QUEUE_NAME, true);
         }

         ClientMessage message = clientSession.createMessage(Message.TEXT_TYPE, true);
         message.putStringProperty("routingType", RoutingType.ANYCAST.name());
         message.putStringProperty("destination", name);
         message.getBodyBuffer().writeUTF(consumerId);

         clientProducer.send(message);
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not delete consumer")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }

   protected void createInternal(String name, String selector, boolean durable) throws ActiveMQException
   {
      ClientSession session = manager.getSessionFactory().createSession(false, false, false);
      try {

         QueueQuery query = session.queueQuery(new SimpleString(name));
         if (!query.isExists()) {
            if (selector != null) {
               session.createQueue(name, name, selector, durable);
            } else {
               session.createQueue(name, name, durable);
            }

         } else {
            throw new WebApplicationException(Response.status(412).type("text/plain").entity("Queue already exists.").build());
         }
      } finally {
         try {
            session.close();
         } catch (Exception ignored) {
         }
      }
   }

   public Map<String, QueueResource> getQueues() {
      return queues;
   }

   @Path("/{queue-name}")
   public synchronized QueueResource findQueue(@PathParam("queue-name") String name) throws Exception {
      QueueResource queue = queues.get(name);
      if (queue == null) {
         String queueName = name;
         ClientSession session = manager.getSessionFactory().createSession(false, false, false);
         try {
            if(!manager.isClustered()) {
               QueueQuery query = session.queueQuery(new SimpleString(queueName));
               if (!query.isExists() ) {
                  throw new WebApplicationException(Response.status(404).type("text/plain").entity("Queue '" + name + "' does not exist").build());
               }
            }
            DestinationSettings queueSettings = manager.getDefaultSettings();
            boolean defaultDurable = queueSettings.isDurableSend();

            queue = createQueueResource(queueName, defaultDurable, queueSettings.getConsumerSessionTimeoutSeconds(), queueSettings.isDuplicatesAllowed());
         } finally {
            try {
               session.close();
            } catch (ActiveMQException e) {
            }
         }
      }
      return queue;
   }

   public QueueResource createQueueResource(String queueName,
                                            boolean defaultDurable,
                                            int timeoutSeconds,
                                            boolean duplicates) throws Exception {
      QueueResource queueResource = new QueueResource();
      queueResource.setQueueDestinationsResource(this);
      queueResource.setDestination(queueName);
      queueResource.setServiceManager(manager);

      ConsumersResource consumers = new ConsumersResource();
      consumers.setConsumerTimeoutSeconds(timeoutSeconds);
      consumers.setDestination(queueName);
      consumers.setSessionFactory(manager.getConsumerSessionFactory());
      consumers.setServiceManager(manager);
      queueResource.setConsumers(consumers);

      PushConsumerResource push = new PushConsumerResource();
      push.setDestination(queueName);
      push.setSessionFactory(manager.getConsumerSessionFactory());
      push.setJmsOptions(manager.getJmsOptions());
      queueResource.setPushConsumers(push);

      PostMessage sender = null;
      if (duplicates) {
         sender = new PostMessageDupsOk();
      } else {
         sender = new PostMessageNoDups();
      }
      sender.setServiceManager(manager);
      sender.setDefaultDurable(defaultDurable);
      sender.setDestination(queueName);
      sender.setSessionFactory(manager.getSessionFactory());
      sender.setPoolSize(manager.getProducerPoolSize());
      sender.setProducerTimeToLive(manager.getProducerTimeToLive());
      sender.init();
      queueResource.setSender(sender);

      if (manager.getPushStore() != null) {
         push.setPushStore(manager.getPushStore());
         List<PushRegistration> regs = manager.getPushStore().getByDestination(queueName);
         for (PushRegistration reg : regs) {
            push.addRegistration(reg);
         }
      }

      queueResource.start();
      getQueues().put(queueName, queueResource);
      return queueResource;
   }
}

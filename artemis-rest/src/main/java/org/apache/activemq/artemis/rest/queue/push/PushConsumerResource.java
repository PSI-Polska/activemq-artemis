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
package org.apache.activemq.artemis.rest.queue.push;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.QueueDestinationsResource;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

public class PushConsumerResource implements MessageHandler {

   protected Map<String, PushConsumer> consumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected final AtomicLong sessionCounter = new AtomicLong(1);
   protected PushStore pushStore;

   private ConnectionFactoryOptions jmsOptions;
   private ClientSession session;
   private ClientConsumer consumer;

   @Override
   public void onMessage(ClientMessage message) {
      String consumerId = message.getBodyBuffer().readUTF();
      deleteConsumer(consumerId);
   }

   public void start() {
      try {
         session = sessionFactory.createSession(true, true);

         ClientSession.AddressQuery addressQuery = session.addressQuery(QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            session.createAddress(QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         ClientSession.QueueQuery queueQuery = session.queueQuery(QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            session.createQueue(QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME, true);
         }

         SimpleString filter = new SimpleString(String.format("routingType = '%s' AND destination = '%s'", RoutingType.ANYCAST.name(), destination));
         consumer = session.createConsumer(QueueDestinationsResource.UNREGISTRATION_QUEUE_NAME, filter);
         consumer.setMessageHandler(this);

         session.start();
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not create session", ex);
      }
   }

   public void stop() {
      try {
         if (consumer != null && !consumer.isClosed()) {
            consumer.close();
         }
         if (session != null && !session.isClosed()) {
            session.close();
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not close consumer or session", ex);
      }

      for (PushConsumer consumer : consumers.values()) {
         consumer.stop();
      }
   }

   public PushStore getPushStore() {
      return pushStore;
   }

   public void setPushStore(PushStore pushStore) {
      this.pushStore = pushStore;
   }

   public void addRegistration(PushRegistration reg) throws Exception {
      if (reg.isEnabled() == false)
         return;
      PushConsumer consumer = new PushConsumer(sessionFactory, destination, reg.getId(), reg, pushStore, jmsOptions);
      consumer.start();
      consumers.put(reg.getId(), consumer);
   }

   @POST
   @Consumes("application/xml")
   public Response create(@Context UriInfo uriInfo, PushRegistration registration) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      String generatedId = performCreation(registration);
      
      UriBuilder location = uriInfo.getAbsolutePathBuilder();
      location.path(generatedId);
      return Response.created(location.build()).build();
   }

    public String performCreation(PushRegistration registration) throws WebApplicationException
    {
        // todo put some logic here to check for duplicates
        String genId = sessionCounter.getAndIncrement() + "-" + startup;
        registration.setId(genId);
        registration.setDestination(destination);
        PushConsumer consumer = new PushConsumer(sessionFactory, destination, genId, registration, pushStore, jmsOptions);
        try {
            consumer.start();
            if (registration.isDurable() && pushStore != null) {
                pushStore.add(registration);
            }
        } catch (Exception e) {
            consumer.stop();
            throw new WebApplicationException(e, Response.serverError().entity("Failed to start consumer.").type("text/plain").build());
        } consumers.put(genId, consumer);
        return genId;
    }

   @GET
   @Path("{consumer-id}")
   @Produces("application/xml")
   public PushRegistration getConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      PushConsumer consumer = consumers.get(consumerId);
      if (consumer == null) {
         throw new WebApplicationException(Response.status(404).entity("Could not find consumer.").type("text/plain").build());
      }
      return consumer.getRegistration();
   }

   public void deleteConsumer(String consumerId) {
      ActiveMQRestLogger.LOGGER.removingPushConsumer(consumerId, destination);

      PushConsumer consumer = consumers.remove(consumerId);
      if (consumer == null) {
         ActiveMQRestLogger.LOGGER.pushConsumerDoesNotExist(consumerId, destination);
      }
      consumer.stop();
   }

   public Map<String, PushConsumer> getConsumers() {
      return consumers;
   }

   public ClientSessionFactory getSessionFactory() {
      return sessionFactory;
   }

   public void setSessionFactory(ClientSessionFactory sessionFactory) {
      this.sessionFactory = sessionFactory;
   }

   public String getDestination() {
      return destination;
   }

   public void setDestination(String destination) {
      this.destination = destination;
   }

   public void setJmsOptions(ConnectionFactoryOptions jmsOptions) {
      this.jmsOptions = jmsOptions;
   }
}

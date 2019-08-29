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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumer;

public class PushSubscriptionsResource implements MessageHandler {

   protected Map<String, PushSubscription> consumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected final AtomicLong sessionCounter = new AtomicLong(1);
   protected TopicPushStore pushStore;

   private ConnectionFactoryOptions jmsOptions;
   private ClientSession session;
   private ClientConsumer consumer;

   public PushSubscriptionsResource(ConnectionFactoryOptions jmsOptions) {
      this.jmsOptions = jmsOptions;
   }

   @Override
   public void onMessage(ClientMessage message) {
      String consumerId = message.getBodyBuffer().readUTF();
      deleteConsumer(consumerId);
   }

   public void start() {
      try {
         session = sessionFactory.createSession(true, true);

         ClientSession.AddressQuery addressQuery = session.addressQuery(TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            session.createAddress(TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         ClientSession.QueueQuery queueQuery = session.queueQuery(TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            session.createQueue(TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME, RoutingType.MULTICAST, TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME, true);
         }

         SimpleString filter = new SimpleString(String.format("routingType = '%s' AND destination = '%s'", RoutingType.MULTICAST.name(), destination));
         consumer = session.createConsumer(TopicDestinationsResource.UNREGISTRATION_QUEUE_NAME, filter);
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
         if (consumer.getRegistration().isDurable() == false) {
            deleteSubscriberQueue(consumer);
         }
      }
   }

   public TopicPushStore getPushStore() {
      return pushStore;
   }

   public void setPushStore(TopicPushStore pushStore) {
      this.pushStore = pushStore;
   }

   public ClientSession createSubscription(String subscriptionName, boolean durable) {
      ClientSession session = null;
      try {
         session = sessionFactory.createSession();

         if (durable) {
            session.createQueue(destination, subscriptionName, true);
         } else {
            session.createTemporaryQueue(destination, subscriptionName);
         }
         return session;
      } catch (ActiveMQException e) {
         throw new RuntimeException(e);
      }
   }

   public void addRegistration(PushTopicRegistration reg) throws Exception {
      if (reg.isEnabled() == false)
         return;
      String destination = reg.getDestination();
      ClientSession session = sessionFactory.createSession(false, false, false);
      ClientSession.QueueQuery query = session.queueQuery(new SimpleString(destination));
      ClientSession createSession = null;
      if (!query.isExists()) {
         createSession = createSubscription(destination, reg.isDurable());
      }
      PushSubscription consumer = new PushSubscription(sessionFactory, reg.getDestination(), reg.getId(), reg, pushStore, jmsOptions);
      try {
         consumer.start();
      } catch (Exception e) {
         consumer.stop();
         throw new Exception("Failed starting push subscriber for " + destination + " of push subscriber: " + reg.getTarget(), e);
      } finally {
         closeSession(createSession);
         closeSession(session);
      }

      consumers.put(reg.getId(), consumer);

   }

   private void closeSession(ClientSession createSession) {
      if (createSession != null) {
         try {
            createSession.close();
         } catch (ActiveMQException e) {
         }
      }
   }

   @POST
   public Response create(@Context UriInfo uriInfo, PushTopicRegistration registration) {
      ActiveMQRestLogger.LOGGER.debug("Handling POST request for \"" + uriInfo.getPath() + "\"");

      Optional<PushSubscription> existing = findBySubscriptionId( registration.getDestination() );
      return existing.map( subscription -> restartSubscription( uriInfo, subscription ) ).orElseGet( () -> createNewSubscription( uriInfo, registration ) );
   }

   @GET
   @Path("{consumer-id}")
   @Produces("application/xml")
   public PushTopicRegistration getConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      PushConsumer consumer = consumers.get(consumerId);
      if (consumer == null) {
         throw new WebApplicationException(Response.status(404).entity("Could not find consumer.").type("text/plain").build());
      }
      return (PushTopicRegistration) consumer.getRegistration();
   }

   public void deleteConsumer(String consumerId) {
      ActiveMQRestLogger.LOGGER.removingPushSubscription(consumerId, destination);

      PushConsumer consumer = consumers.remove(consumerId);
      if (consumer == null) {
         ActiveMQRestLogger.LOGGER.pushSubscriptionDoesNotExist(consumerId, destination);
      }
      consumer.stop();
      deleteSubscriberQueue(consumer);
   }

   public Map<String, PushSubscription> getConsumers() {
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

   private Response createNewSubscription(UriInfo uriInfo, PushTopicRegistration registration) {
       
      String genId = performCreationOfSubscription(registration);
      
      UriBuilder location = uriInfo.getAbsolutePathBuilder();
      location.path(genId);
      return Response.created(location.build()).build();
   }

   public void performCreationOrRestartSubscription(PushTopicRegistration registration) {
      try {
         Optional<PushSubscription> existing = findBySubscriptionId(registration.getDestination());
         if (existing.isPresent()) {
            performRestartSubscription(existing.get());
         } else {
            performCreationOfSubscription(registration);
         }
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      }
   }

    private String performCreationOfSubscription(PushTopicRegistration registration) throws WebApplicationException
    {
        String genId = sessionCounter.getAndIncrement() + "-topic-" + destination + "-" + startup;
        if (registration.getDestination() == null) {
            registration.setDestination(genId);
        } 
        registration.setId(genId);
        registration.setTopic(destination);
        registration.setDisableOnFailure( true );
        ClientSession createSession = createSubscription(registration.getDestination(), registration.isDurable());
        try {
            PushSubscription consumer = new PushSubscription(sessionFactory, registration.getDestination(), genId, registration, pushStore, jmsOptions);
            try {
                consumer.start();
                if (registration.isDurable() && pushStore != null) {
                    pushStore.add(registration);
                }
            } catch (Exception e) {
                consumer.stop();
                throw new WebApplicationException(e, Response.serverError().entity("Failed to start consumer.").type("text/plain").build());
            }
            
            consumers.put(genId, consumer);
        } finally {
            closeSession(createSession);
        } return genId;
    }

   private Response restartSubscription(UriInfo uriInfo, PushSubscription subscription) {
      try {
         performRestartSubscription(subscription);
      } catch (Exception e) {
         subscription.stop();
         throw new WebApplicationException(e, Response.serverError().entity("Failed to start consumer.").type("text/plain").build());
      }

      UriBuilder location = uriInfo.getAbsolutePathBuilder();
      location.path(subscription.getRegistration().getId());
      return Response.created(location.build()).build();
   }

   private void performRestartSubscription(PushSubscription subscription) throws Exception {
      subscription.getRegistration().setEnabled( true );

      subscription.start();
      if (subscription.getRegistration().isDurable()) {
         pushStore.update(subscription.getRegistration());
      }
   }

   private Optional<PushSubscription> findBySubscriptionId( String subscriptionId) {

      if (subscriptionId == null) {
         return Optional.empty();
      }

      return consumers.values()
          .stream()
          .filter( consumer -> subscriptionId.equals( consumer.getDestination()) )
          .findAny();
   }

   public void deleteSubscriberQueue(PushConsumer consumer) {
      String subscriptionName = consumer.getDestination();
      ClientSession session = null;
      try {
         session = sessionFactory.createSession();

         session.deleteQueue(subscriptionName);
      } catch (ActiveMQException e) {
      } finally {
         closeSession(session);
      }
   }
}

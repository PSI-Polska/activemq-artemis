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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumer;

public class PushSubscriptionsResource implements MessageHandler {

   private static final SimpleString SUBSCRIPTION_REMOVE_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.push.subscription.remove");
   private static final String SUBSCRIPTION_REMOVE_DESTINATION_PARAM = "destination";

   private static final SimpleString SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.push.subscription.remove.reply");
   private static final String SUBSCRIPTION_REMOVE_REPLY_ID_PARAM = "replyId";
   private static final String SUBSCRIPTION_REMOVE_SUCCESS_FLAG_PARAM = "success";

   protected Map<String, PushSubscription> consumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected final AtomicLong sessionCounter = new AtomicLong(1);
   protected TopicPushStore pushStore;

   private ConnectionFactoryOptions jmsOptions;
   private ClientSession session;

   public PushSubscriptionsResource(ConnectionFactoryOptions jmsOptions) {
      this.jmsOptions = jmsOptions;
   }

   public void start() throws Exception {
      createUnregistrationTopicIfNeeded();

      session = sessionFactory.createSession();

      String filter = String.format("%s = '%s'", SUBSCRIPTION_REMOVE_DESTINATION_PARAM, destination);
      ClientConsumer consumer = session.createConsumer(SUBSCRIPTION_REMOVE_QUEUE_NAME, new SimpleString(filter));
      consumer.setMessageHandler(this);

      session.start();
   }

   public void stop() {
      try {
         if (session != null && !session.isClosed()) {
            session.close();
            session = null;
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not close session", ex);
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

   @DELETE
   @Path("{consumer-id}")
   public void deleteConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      createUnregistrationTopicIfNeeded();

      UUID replyId = UUID.randomUUID();
      String filter = String.format("%s = '%s'", SUBSCRIPTION_REMOVE_REPLY_ID_PARAM, replyId);

      try (ClientSession clientSession = getSessionFactory().createSession();
           ClientProducer clientProducer = clientSession.createProducer(SUBSCRIPTION_REMOVE_QUEUE_NAME);
           ClientConsumer replyConsumer = clientSession.createConsumer(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME, new SimpleString(filter))) {
         ClientMessage message = clientSession.createMessage(Message.TEXT_TYPE, true);
         message.putStringProperty(SUBSCRIPTION_REMOVE_REPLY_ID_PARAM, replyId.toString());
         message.putStringProperty(SUBSCRIPTION_REMOVE_DESTINATION_PARAM, destination);
         message.setReplyTo(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME);
         message.getBodyBuffer().writeUTF(consumerId);

         clientProducer.send(message);

         clientSession.start();

         ClientMessage reply = replyConsumer.receive(TimeUnit.SECONDS.toMillis(8L));
         if (reply != null) {
            reply.acknowledge();
         }

         clientSession.commit();

         if (reply == null) {
            throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                           .entity("Could not delete consumer")
                                                           .type("text/plain")
                                                           .build());
         } else if (!reply.getBooleanProperty("success")) {
            throw new NotFoundException(Response.status(Response.Status.NOT_FOUND)
                                                .entity("Could not find subscription")
                                                .type("text/plain")
                                                .build());
         }

      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not delete consumer")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }

   @Override
   public void onMessage(ClientMessage message) {
      try (ClientSession clientSession = getSessionFactory().createSession();
           ClientProducer producer = clientSession.createProducer(message.getReplyTo())) {
         ClientMessage reply = clientSession.createMessage(ClientMessage.TEXT_TYPE, true);
         reply.putStringProperty(SUBSCRIPTION_REMOVE_REPLY_ID_PARAM, message.getStringProperty(SUBSCRIPTION_REMOVE_REPLY_ID_PARAM));
         try {
            String consumerId = message.getBodyBuffer().readUTF();

            message.acknowledge();
            session.commit();

            deleteConsumer(consumerId);

            reply.putBooleanProperty(SUBSCRIPTION_REMOVE_SUCCESS_FLAG_PARAM, true);
         } catch (Exception ex) {
            reply.putBooleanProperty(SUBSCRIPTION_REMOVE_SUCCESS_FLAG_PARAM, false);

            ActiveMQRestLogger.LOGGER.warn("Could not remove subscription", ex);
         } finally {
            reply.setExpiration(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L));
            producer.send(reply);
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not send reply", ex);
      }
   }

   private void deleteConsumer(String consumerId) {
      ActiveMQRestLogger.LOGGER.removingPushSubscription(consumerId, destination);

      PushConsumer consumer = consumers.remove(consumerId);
      if (consumer == null) {
         throw new NullPointerException("Could not find subscription");
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

   private synchronized void createUnregistrationTopicIfNeeded() {
      try (ClientSession clientSession = getSessionFactory().createSession(false, false, false)) {
         ClientSession.AddressQuery addressQuery = clientSession.addressQuery(SUBSCRIPTION_REMOVE_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(SUBSCRIPTION_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         addressQuery = clientSession.addressQuery(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, false);
         }

         ClientSession.QueueQuery queueQuery = clientSession.queueQuery(SUBSCRIPTION_REMOVE_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(SUBSCRIPTION_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, SUBSCRIPTION_REMOVE_QUEUE_NAME, true);
         }

         queueQuery = clientSession.queueQuery(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, SUBSCRIPTION_REMOVE_REPLY_QUEUE_NAME, true);
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not create subscription removal destination")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }
}

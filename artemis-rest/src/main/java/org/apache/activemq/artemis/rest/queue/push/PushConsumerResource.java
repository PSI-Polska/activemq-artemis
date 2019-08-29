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

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.Map;
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
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;

public class PushConsumerResource implements MessageHandler {

   private static final SimpleString CONSUMER_REMOVE_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.push.consumer.remove");
   private static final String CONSUMER_REMOVE_DESTINATION_PARAM = "destination";

   private static final SimpleString CONSUMER_REMOVE_REPLY_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.push.consumer.remove.reply");
   private static final String CONSUMER_REMOVE_REPLY_ID_PARAM = "replyId";
   private static final String CONSUMER_REMOVE_SUCCESS_FLAG_PARAM = "success";

   protected Map<String, PushConsumer> consumers = new ConcurrentHashMap<>();
   protected ClientSessionFactory sessionFactory;
   protected String destination;
   protected final String startup = Long.toString(System.currentTimeMillis());
   protected final AtomicLong sessionCounter = new AtomicLong(1);
   protected PushStore pushStore;

   private ConnectionFactoryOptions jmsOptions;
   private ClientSession session;

   public void start() throws Exception {
      createUnregistrationTopicIfNeeded();

      session = sessionFactory.createSession();

      String filter = String.format("%s = '%s'", CONSUMER_REMOVE_DESTINATION_PARAM, destination);
      ClientConsumer consumer = session.createConsumer(CONSUMER_REMOVE_QUEUE_NAME, new SimpleString(filter));
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

   @DELETE
   @Path("{consumer-id}")
   public void deleteConsumer(@Context UriInfo uriInfo, @PathParam("consumer-id") String consumerId) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      createUnregistrationTopicIfNeeded();

      UUID replyId = UUID.randomUUID();
      String filter = String.format("%s = '%s'", CONSUMER_REMOVE_REPLY_ID_PARAM, replyId);

      try (ClientSession clientSession = getSessionFactory().createSession();
           ClientProducer clientProducer = clientSession.createProducer(CONSUMER_REMOVE_QUEUE_NAME);
           ClientConsumer replyConsumer = clientSession.createConsumer(CONSUMER_REMOVE_REPLY_QUEUE_NAME, new SimpleString(filter))) {
         ClientMessage message = clientSession.createMessage(Message.TEXT_TYPE, true);
         message.putStringProperty(CONSUMER_REMOVE_REPLY_ID_PARAM, replyId.toString());
         message.putStringProperty(CONSUMER_REMOVE_DESTINATION_PARAM, destination);
         message.setReplyTo(CONSUMER_REMOVE_REPLY_QUEUE_NAME);
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
                                                .entity("Could not find consumer.")
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
         reply.putStringProperty(CONSUMER_REMOVE_REPLY_ID_PARAM, message.getStringProperty(CONSUMER_REMOVE_REPLY_ID_PARAM));
         try {
            String consumerId = message.getBodyBuffer().readUTF();

            message.acknowledge();
            session.commit();

            deleteConsumer(consumerId);

            reply.putBooleanProperty(CONSUMER_REMOVE_SUCCESS_FLAG_PARAM, true);
         } catch (Exception ex) {
            reply.putBooleanProperty(CONSUMER_REMOVE_SUCCESS_FLAG_PARAM, false);

            ActiveMQRestLogger.LOGGER.warn("Could not remove consumer", ex);
         } finally {
            reply.setExpiration(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L));
            producer.send(reply);
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not send reply", ex);
      }
   }

   public void deleteConsumer(String consumerId) {
      ActiveMQRestLogger.LOGGER.removingPushConsumer(consumerId, destination);

      PushConsumer consumer = consumers.remove(consumerId);
      if (consumer == null) {
         throw new NullPointerException("Could not find consumer.");
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

   private synchronized void createUnregistrationTopicIfNeeded() {
      try (ClientSession clientSession = getSessionFactory().createSession(false, false, false)) {
         ClientSession.AddressQuery addressQuery = clientSession.addressQuery(CONSUMER_REMOVE_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(CONSUMER_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         addressQuery = clientSession.addressQuery(CONSUMER_REMOVE_REPLY_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(CONSUMER_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, false);
         }

         ClientSession.QueueQuery queueQuery = clientSession.queueQuery(CONSUMER_REMOVE_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(CONSUMER_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, CONSUMER_REMOVE_QUEUE_NAME, true);
         }

         queueQuery = clientSession.queueQuery(CONSUMER_REMOVE_REPLY_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(CONSUMER_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, CONSUMER_REMOVE_REPLY_QUEUE_NAME, true);
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not create queue removal destination")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }
}

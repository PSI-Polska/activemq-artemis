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

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.DestinationResource;
import org.apache.activemq.artemis.rest.queue.PostMessage;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TopicResource extends DestinationResource implements MessageHandler {

   private static final SimpleString TOPIC_REMOVE_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.topic.remove");
   private static final String TOPIC_REMOVE_DESTINATION_PARAM = "destination";

   private static final SimpleString TOPIC_REMOVE_REPLY_QUEUE_NAME = new SimpleString("org.apache.activemq.artemis.rest.topic.remove.reply");
   private static final String TOPIC_REMOVE_REPLY_ID_PARAM = "replyId";
   private static final String TOPIC_REMOVE_SUCCESS_FLAG_PARAM = "success";

   protected SubscriptionsResource subscriptions;
   protected PushSubscriptionsResource pushSubscriptions;
   private TopicDestinationsResource topicDestinationsResource;

   private ClientSession session;

   public void start() throws Exception {
      createRemovalTopicIfNeeded();

      ClientSessionFactory sessionFactory = serviceManager.getConsumerSessionFactory();
      session = sessionFactory.createSession(true, true);


      String filter = String.format("%s = '%s'", TOPIC_REMOVE_DESTINATION_PARAM, destination);
      ClientConsumer consumer = session.createConsumer(TOPIC_REMOVE_QUEUE_NAME, new SimpleString(filter));
      consumer.setMessageHandler(this);

      session.start();

      pushSubscriptions.start();
   }

   public void stop() {
      subscriptions.stop();
      pushSubscriptions.stop();
      sender.cleanup();

      try {
         if (session != null && !session.isClosed()) {
            session.close();
            session = null;
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not close session", ex);
      }
   }

   @GET
   @Produces("application/xml")
   public Response get(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + uriInfo.getPath() + "\"");

      StringBuilder msg = new StringBuilder();
      msg.append("<topic>").append("<name>").append(destination).append("</name>").append("<atom:link rel=\"create\" href=\"").append(createSenderLink(uriInfo)).append("\"/>").append("<atom:link rel=\"create-with-id\" href=\"").append(createSenderWithIdLink(uriInfo)).append("\"/>").append("<atom:link rel=\"pull-consumers\" href=\"").append(createSubscriptionsLink(uriInfo)).append("\"/>").append("<atom:link rel=\"push-consumers\" href=\"").append(createPushSubscriptionsLink(uriInfo)).append("\"/>")

         .append("</topic>");

      Response.ResponseBuilder builder = Response.ok(msg.toString());
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setSubscriptionsLink(builder, uriInfo);
      setPushSubscriptionsLink(builder, uriInfo);
      return builder.build();
   }

   @HEAD
   @Produces("application/xml")
   public Response head(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getPath() + "\"");

      Response.ResponseBuilder builder = Response.ok();
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setSubscriptionsLink(builder, uriInfo);
      setPushSubscriptionsLink(builder, uriInfo);
      return builder.build();
   }

   protected void setSenderLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createSenderLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create", "create", uri, null);
   }

   protected String createSenderLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setSenderWithIdLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createSenderWithIdLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "create-with-id", "create-with-id", uri, null);
   }

   protected String createSenderWithIdLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("create");
      String uri = builder.build().toString();
      uri += "/{id}";
      return uri;
   }

   protected void setSubscriptionsLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createSubscriptionsLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "pull-subscriptions", "pull-subscriptions", uri, null);
   }

   protected String createSubscriptionsLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("pull-subscriptions");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setPushSubscriptionsLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createPushSubscriptionsLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "push-subscriptions", "push-subscriptions", uri, null);
   }

   protected String createPushSubscriptionsLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("push-subscriptions");
      String uri = builder.build().toString();
      return uri;
   }

   public void setSubscriptions(SubscriptionsResource subscriptions) {
      this.subscriptions = subscriptions;
   }

   @Path("create")
   public PostMessage post() throws Exception {
      return sender;
   }

   @Path("pull-subscriptions")
   public SubscriptionsResource getSubscriptions() {
      return subscriptions;
   }

   @Path("push-subscriptions")
   public PushSubscriptionsResource getPushSubscriptions() {
      return pushSubscriptions;
   }

   public void setPushSubscriptions(PushSubscriptionsResource pushSubscriptions) {
      this.pushSubscriptions = pushSubscriptions;
   }

   @DELETE
   public void deleteTopic(@Context UriInfo uriInfo) throws Exception {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      createRemovalTopicIfNeeded();

      UUID replyId = UUID.randomUUID();
      String filter = String.format("%s = '%s'", TOPIC_REMOVE_REPLY_ID_PARAM, replyId);

      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(true, true);
           ClientProducer clientProducer = clientSession.createProducer(TOPIC_REMOVE_QUEUE_NAME);
           ClientConsumer replyConsumer = clientSession.createConsumer(TOPIC_REMOVE_REPLY_QUEUE_NAME, new SimpleString(filter))) {
         ClientMessage message = clientSession.createMessage(Message.TEXT_TYPE, true);
         message.putStringProperty(TOPIC_REMOVE_DESTINATION_PARAM, destination);
         message.putStringProperty(TOPIC_REMOVE_REPLY_ID_PARAM, replyId.toString());
         message.setReplyTo(TOPIC_REMOVE_REPLY_QUEUE_NAME);

         clientProducer.send(message);

         clientSession.start();

         ClientMessage reply = replyConsumer.receive(TimeUnit.SECONDS.toMillis(8L));
         if (reply != null) {
            reply.acknowledge();
         }

         clientSession.commit();

         if (reply == null || !reply.getBooleanProperty(TOPIC_REMOVE_SUCCESS_FLAG_PARAM)) {
            throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                           .entity("Could not remove topic")
                                                           .type("text/plain")
                                                           .build());
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not remove topic")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }

   @Override
   public void onMessage(ClientMessage message) {
      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(true, true);
           ClientProducer producer = clientSession.createProducer(message.getReplyTo())) {
         ClientMessage reply = clientSession.createMessage(ClientMessage.TEXT_TYPE, true);
         reply.putStringProperty(TOPIC_REMOVE_REPLY_ID_PARAM, message.getStringProperty(TOPIC_REMOVE_REPLY_ID_PARAM));
         try {
            message.acknowledge();
            session.commit();

            deleteTopic();

            reply.putBooleanProperty(TOPIC_REMOVE_SUCCESS_FLAG_PARAM, true);
         } catch (Exception ex) {
            reply.putBooleanProperty(TOPIC_REMOVE_SUCCESS_FLAG_PARAM, false);

            ActiveMQRestLogger.LOGGER.warn("Could not remove topic", ex);
         } finally {
            reply.setExpiration(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L));
            producer.send(reply);
         }
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not send reply", ex);
      }
   }

   public void deleteTopic() throws ActiveMQException {
      ActiveMQRestLogger.LOGGER.removingTopic(destination);

      topicDestinationsResource.getTopics().remove(destination);

      try {
         stop();
      } catch (Exception ignored) {
      }

      try (ClientSession session = serviceManager.getSessionFactory().createSession(false, false, false)) {
         SimpleString topicName = new SimpleString(destination);
         session.deleteQueue(topicName);
      }

   }

   public void setTopicDestinationsResource(TopicDestinationsResource topicDestinationsResource) {
      this.topicDestinationsResource = topicDestinationsResource;
   }

   private synchronized void createRemovalTopicIfNeeded() {
      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(false, false, false)) {
         ClientSession.AddressQuery addressQuery = clientSession.addressQuery(TOPIC_REMOVE_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(TOPIC_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, false);
         }

         addressQuery = clientSession.addressQuery(TOPIC_REMOVE_REPLY_QUEUE_NAME);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(TOPIC_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, false);
         }

         ClientSession.QueueQuery queueQuery = clientSession.queueQuery(TOPIC_REMOVE_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(TOPIC_REMOVE_QUEUE_NAME, RoutingType.MULTICAST, TOPIC_REMOVE_QUEUE_NAME, true);
         }

         queueQuery = clientSession.queueQuery(TOPIC_REMOVE_REPLY_QUEUE_NAME);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(TOPIC_REMOVE_REPLY_QUEUE_NAME, RoutingType.ANYCAST, TOPIC_REMOVE_REPLY_QUEUE_NAME, true);
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not create topic removal destination")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }
}

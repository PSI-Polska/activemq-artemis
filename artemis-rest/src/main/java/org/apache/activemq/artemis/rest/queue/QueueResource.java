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

import javax.servlet.http.HttpServletRequest;
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
import org.apache.activemq.artemis.api.core.client.ClientSession.AddressQuery;
import org.apache.activemq.artemis.api.core.client.ClientSession.QueueQuery;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.PushConsumerResource;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class QueueResource extends DestinationResource implements MessageHandler {

   private enum QueueRemoveStatus {
      SUCCESS, FAILURE
   }

   private static final SimpleString QUEUE_REMOVE_ADDRESS = new SimpleString("org.apache.activemq.artemis.rest.queue.remove");
   private static final String QUEUE_REMOVE_DESTINATION_PARAM = "destination";

   private static final SimpleString QUEUE_REMOVE_REPLY_ADDRESS = new SimpleString("org.apache.activemq.artemis.rest.queue.remove.reply");
   private static final String QUEUE_REMOVE_REPLY_ID_PARAM = "replyId";

   private final SimpleString queueRemoveQueueName = new SimpleString(UUID.randomUUID().toString());
   private final SimpleString queueRemoveReplyQueueName = new SimpleString(UUID.randomUUID().toString());

   protected ConsumersResource consumers;
   protected PushConsumerResource pushConsumers;
   private QueueDestinationsResource queueDestinationsResource;

   private ClientSession session;

   public void start() throws Exception {
      createRemovalTopicIfNeeded();

      ClientSessionFactory sessionFactory = serviceManager.getConsumerSessionFactory();
      session = sessionFactory.createSession(true, true);

      ClientConsumer consumer = session.createConsumer(queueRemoveQueueName);
      consumer.setMessageHandler(this);

      session.start();

      pushConsumers.start();
   }

   public void stop() {
      consumers.stop();
      pushConsumers.stop();
      sender.cleanup();

      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(false, false, false)) {
         if (session != null && !session.isClosed()) {
            session.close();
            session = null;
         }

         clientSession.deleteQueue(queueRemoveQueueName);
         clientSession.deleteQueue(queueRemoveReplyQueueName);
      } catch (ActiveMQException ex) {
         ActiveMQRestLogger.LOGGER.error("Could not close session", ex);
      }
   }

   @GET
   @Produces("application/xml")
   public Response get(@Context UriInfo uriInfo, @Context HttpServletRequest requestContext) {
      ActiveMQRestLogger.LOGGER.debug("Handling GET request for \"" + destination + "\" from " + requestContext.getRemoteAddr() + ":" + requestContext.getRemotePort());

      StringBuilder msg = new StringBuilder();
      msg.append("<queue>").append("<name>").append(destination).append("</name>").append("<atom:link rel=\"create\" href=\"").append(createSenderLink(uriInfo)).append("\"/>").append("<atom:link rel=\"create-with-id\" href=\"").append(createSenderWithIdLink(uriInfo)).append("\"/>").append("<atom:link rel=\"pull-consumers\" href=\"").append(createConsumersLink(uriInfo)).append("\"/>").append("<atom:link rel=\"push-consumers\" href=\"").append(createPushConsumersLink(uriInfo)).append("\"/>")

         .append("</queue>");

      Response.ResponseBuilder builder = Response.ok(msg.toString());
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
      return builder.build();
   }

   @HEAD
   @Produces("application/xml")
   public Response head(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling HEAD request for \"" + uriInfo.getRequestUri() + "\"");

      Response.ResponseBuilder builder = Response.ok();
      setSenderLink(builder, uriInfo);
      setSenderWithIdLink(builder, uriInfo);
      setConsumersLink(builder, uriInfo);
      setPushConsumersLink(builder, uriInfo);
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

   protected void setConsumersLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "pull-consumers", "pull-consumers", uri, null);
   }

   protected String createConsumersLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("pull-consumers");
      String uri = builder.build().toString();
      return uri;
   }

   protected void setPushConsumersLink(Response.ResponseBuilder response, UriInfo info) {
      String uri = createPushConsumersLink(info);
      serviceManager.getLinkStrategy().setLinkHeader(response, "push-consumers", "push-consumers", uri, null);
   }

   protected String createPushConsumersLink(UriInfo info) {
      UriBuilder builder = info.getRequestUriBuilder();
      builder.path("push-consumers");
      String uri = builder.build().toString();
      return uri;
   }

   public void setConsumers(ConsumersResource consumers) {
      this.consumers = consumers;
   }

   @Path("create")
   public PostMessage post() throws Exception {
      return sender;
   }

   @Path("pull-consumers")
   public ConsumersResource getConsumers() {
      return consumers;
   }

   public void setPushConsumers(PushConsumerResource pushConsumers) {
      this.pushConsumers = pushConsumers;
   }

   @Path("push-consumers")
   public PushConsumerResource getPushConsumers() {
      return pushConsumers;
   }

   public void setQueueDestinationsResource(QueueDestinationsResource queueDestinationsResource) {
      this.queueDestinationsResource = queueDestinationsResource;
   }

   @DELETE
   public void deleteQueue(@Context UriInfo uriInfo) {
      ActiveMQRestLogger.LOGGER.debug("Handling DELETE request for \"" + uriInfo.getPath() + "\"");

      UUID replyId = UUID.randomUUID();
      String filter = String.format("%s = '%s'", QUEUE_REMOVE_REPLY_ID_PARAM, replyId);

      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession();
           ClientProducer clientProducer = clientSession.createProducer(QUEUE_REMOVE_ADDRESS);
           ClientConsumer replyConsumer = clientSession.createConsumer(queueRemoveReplyQueueName, new SimpleString(filter))) {
         ClientMessage message = clientSession.createMessage(Message.TEXT_TYPE, true);
         message.putStringProperty(QUEUE_REMOVE_REPLY_ID_PARAM, replyId.toString());
         message.putStringProperty(QUEUE_REMOVE_DESTINATION_PARAM, destination);
         message.setReplyTo(QUEUE_REMOVE_REPLY_ADDRESS);

         clientProducer.send(message);

         clientSession.start();

         ClientMessage reply = replyConsumer.receive(TimeUnit.SECONDS.toMillis(5L));

         QueueRemoveStatus status = QueueRemoveStatus.FAILURE;
         if (reply != null) {
            reply.acknowledge();
            status = QueueRemoveStatus.valueOf(reply.getBodyBuffer().readUTF());
         }

         clientSession.commit();

         if (status == QueueRemoveStatus.FAILURE) {
            throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                           .entity("Could not remove queue")
                                                           .type("text/plain")
                                                           .build());
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not remove queue")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }

   @Override
   public void onMessage(ClientMessage message) {
      try {
         message.acknowledge();
         session.commit();

         String queueName = message.getStringProperty(QUEUE_REMOVE_DESTINATION_PARAM);

         ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
         try (ClientSession clientSession = sessionFactory.createSession(true, true);
              ClientProducer producer = clientSession.createProducer(message.getReplyTo())) {

            QueueQuery queueQuery = clientSession.queueQuery(new SimpleString(queueName));
            if (queueQuery.isExists()) {
               QueueRemoveStatus status = deleteQueue();

               ClientMessage reply = clientSession.createMessage(ClientMessage.TEXT_TYPE, true);
               reply.putStringProperty(QUEUE_REMOVE_DESTINATION_PARAM, message.getStringProperty(QUEUE_REMOVE_DESTINATION_PARAM));
               reply.putStringProperty(QUEUE_REMOVE_REPLY_ID_PARAM, message.getStringProperty(QUEUE_REMOVE_REPLY_ID_PARAM));
               reply.getBodyBuffer().writeUTF(status.name());
               reply.setExpiration(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L));
               producer.send(reply);
            }
         }
      } catch (Exception ex) {
         ActiveMQRestLogger.LOGGER.error("Could not send reply", ex);
      }
   }

   private QueueRemoveStatus deleteQueue() {
      try {
         ActiveMQRestLogger.LOGGER.removingQueue(destination);

         queueDestinationsResource.getQueues().remove(destination);

         stop();

         try (ClientSession session = serviceManager.getSessionFactory().createSession(false, false, false)) {
            SimpleString queueName = new SimpleString(destination);
            session.deleteQueue(queueName);
         }

         return QueueRemoveStatus.SUCCESS;
      } catch (Exception ex) {
         ActiveMQRestLogger.LOGGER.error("Could not remove queue", ex);

         return QueueRemoveStatus.FAILURE;
      }
   }

   private synchronized void createRemovalTopicIfNeeded() {
      ClientSessionFactory sessionFactory = serviceManager.getSessionFactory();
      try (ClientSession clientSession = sessionFactory.createSession(false, false, false)) {
         AddressQuery addressQuery = clientSession.addressQuery(QUEUE_REMOVE_ADDRESS);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(QUEUE_REMOVE_ADDRESS, RoutingType.MULTICAST, false);
         }

         addressQuery = clientSession.addressQuery(QUEUE_REMOVE_REPLY_ADDRESS);
         if (!addressQuery.isExists()) {
            clientSession.createAddress(QUEUE_REMOVE_REPLY_ADDRESS, RoutingType.MULTICAST, false);
         }

         String filter = String.format("%s = '%s'", QUEUE_REMOVE_DESTINATION_PARAM, destination);

         QueueQuery queueQuery = clientSession.queueQuery(queueRemoveQueueName);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(QUEUE_REMOVE_ADDRESS, RoutingType.MULTICAST, queueRemoveQueueName, new SimpleString(filter), true);
         }

         queueQuery = clientSession.queueQuery(queueRemoveReplyQueueName);
         if (!queueQuery.isExists()) {
            clientSession.createQueue(QUEUE_REMOVE_REPLY_ADDRESS, RoutingType.MULTICAST, queueRemoveReplyQueueName, new SimpleString(filter), true);
         }
      } catch (ActiveMQException ex) {
         throw new InternalServerErrorException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                                                        .entity("Could not create queue removal destination")
                                                        .type("text/plain")
                                                        .build(), ex);
      }
   }
}

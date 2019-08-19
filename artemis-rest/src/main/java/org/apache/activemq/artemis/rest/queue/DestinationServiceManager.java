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
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistrationUnmarshaler;
import org.apache.activemq.artemis.rest.util.LinkStrategy;
import org.apache.activemq.artemis.rest.util.TimeoutTask;
import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public abstract class DestinationServiceManager {

   private static final String GLOBAL_STORAGE_QUEUE_PREFIX = "org.apache.activemq.artemis.rest.pushStore";
   private final String queueSuffix;

   protected ServerLocator locator;
   protected ClientSessionFactory sessionFactory;
   protected ServerLocator consumerServerLocator;
   protected ClientSessionFactory consumerSessionFactory;
   protected boolean started;
   protected String pushStoreFile;
   protected String pushStoreQueuePrefix;
   protected DestinationSettings defaultSettings = DestinationSettings.defaultSettings;
   protected TimeoutTask timeoutTask;
   protected int producerPoolSize;
   protected long producerTimeToLive;
   protected LinkStrategy linkStrategy;
   protected BindingRegistry registry;
   protected ConnectionFactoryOptions jmsOptions;
   protected ClientSession globalQueueSession;
   protected PushRegistrationUnmarshaler unmarshaler;

   public DestinationServiceManager(ConnectionFactoryOptions jmsOptions, String queueSuffix)
   {
      this.jmsOptions = jmsOptions;
      this.queueSuffix = queueSuffix;

      this.unmarshaler = new PushRegistrationUnmarshaler();
   }

   public BindingRegistry getRegistry() {
      return registry;
   }

   public void setRegistry(BindingRegistry registry) {
      this.registry = registry;
   }

   public LinkStrategy getLinkStrategy() {
      return linkStrategy;
   }

   public void setLinkStrategy(LinkStrategy linkStrategy) {
      this.linkStrategy = linkStrategy;
   }

   public long getProducerTimeToLive() {
      return producerTimeToLive;
   }

   public void setProducerTimeToLive(long producerTimeToLive) {
      this.producerTimeToLive = producerTimeToLive;
   }

   public int getProducerPoolSize() {
      return producerPoolSize;
   }

   public void setProducerPoolSize(int producerPoolSize) {
      this.producerPoolSize = producerPoolSize;
   }

   public ClientSessionFactory getConsumerSessionFactory() {
      return consumerSessionFactory;
   }

   public void setConsumerSessionFactory(ClientSessionFactory consumerSessionFactory) {
      this.consumerSessionFactory = consumerSessionFactory;
   }

   /**
    * @return the consumerServerLocator
    */
   public ServerLocator getConsumerServerLocator() {
      return consumerServerLocator;
   }

   /**
    * @param consumerServerLocator the consumerServerLocator to set
    */
   public void setConsumerServerLocator(ServerLocator consumerServerLocator) {
      this.consumerServerLocator = consumerServerLocator;
   }

   public TimeoutTask getTimeoutTask() {
      return timeoutTask;
   }

   public void setTimeoutTask(TimeoutTask timeoutTask) {
      this.timeoutTask = timeoutTask;
   }

   public DestinationSettings getDefaultSettings() {
      return defaultSettings;
   }

   public void setDefaultSettings(DestinationSettings defaultSettings) {
      this.defaultSettings = defaultSettings;
   }

   public ServerLocator getServerLocator() {
      return this.locator;
   }

   public void setServerLocator(ServerLocator locator) {
      this.locator = locator;
   }

   public ClientSessionFactory getSessionFactory() {
      return sessionFactory;
   }

   public void setSessionFactory(ClientSessionFactory sessionFactory) {
      this.sessionFactory = sessionFactory;
   }

   public String getPushStoreFile() {
      return pushStoreFile;
   }

   public void setPushStoreFile(String pushStoreFile) {
      this.pushStoreFile = pushStoreFile;
   }

   public void setPushStoreQueue(String pushStoreQueue){
      this.pushStoreQueuePrefix = pushStoreQueue;
   }

    public String getPushStoreQueue(){
      return pushStoreQueuePrefix;
   }


    protected <T extends DestinationResource> T restartDestinationResource( Callable<T> action) {
      try
      {
         return action.call();
      }
      catch( Exception aE )
      {
         ActiveMQRestLogger.LOGGER.errorRestartingResource( aE);
         throw new RuntimeException( aE );
      }
   }

   protected final String getGlobalQueueName(){
      return GLOBAL_STORAGE_QUEUE_PREFIX+"."+queueSuffix;
   }

   protected String getPrivateQueueName(){
      return pushStoreQueuePrefix+"."+queueSuffix;
   }


   protected void startGlobalQueueHandler( Consumer< PushRegistration > calback )
   {
      try
      {
         if( globalQueueSession != null )
         {
            globalQueueSession.close();
         }
         globalQueueSession = sessionFactory.createSession( false, true );
         ClientConsumer globalQueueConsumer = globalQueueSession.createConsumer( getGlobalQueueName() );
         globalQueueConsumer.setMessageHandler( ( ClientMessage message ) ->
                                                {
                                                   try
                                                   {
                                                      PushRegistration
                                                          content = unmarshaler.unmarshallPushRegistration(
                                                          message.getBodyBuffer().readUTF() );
                                                      calback.accept( content );
                                                      message.acknowledge();
                                                   }
                                                   catch( Exception ex )
                                                   {
                                                      throw new RuntimeException( ex );
                                                   }
                                                } );
         globalQueueSession.start();
      }
      catch( ActiveMQException ex )
      {
         throw new RuntimeException( ex );
      }
   }

   protected void initDefaults() {
      if (locator == null) {
         locator = ActiveMQClient.createServerLocatorWithoutHA(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      }
      if (sessionFactory == null) {
         try {
            sessionFactory = locator.createSessionFactory();
         } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
         }
      }

      if (consumerSessionFactory == null)
         consumerSessionFactory = sessionFactory;

      if (timeoutTask == null)
         throw new RuntimeException("TimeoutTask is not set");
   }

   public abstract void start() throws Exception;

   public abstract void stop();

   public ConnectionFactoryOptions getJmsOptions() {
      return jmsOptions;
   }

   public boolean isClustered() {
      return getSessionFactory().getServerLocator().getTopology().getMembers().size() > 1;
   }
}

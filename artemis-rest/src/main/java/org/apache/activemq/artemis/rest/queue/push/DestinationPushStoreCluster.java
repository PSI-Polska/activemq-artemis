package org.apache.activemq.artemis.rest.queue.push;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DestinationPushStoreCluster
{
    
    private static final String GLOBAL_INFO_TOPIC_PREFIX = "org.apache.activemq.artemis.rest.cluster";
    private static final long CHECK_PERIOD = 10000;
    
    private final ScheduledExecutorService sheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final JAXBContext ctx = JAXBContext.newInstance(InstanceWorkingInfo.class);
    private final Map<String,Long> instances = new ConcurrentHashMap<>();
    
    private final ClientSessionFactory factory;
    private ClientSession keepAliveListenerSession;
    
    private final String privateQueueName;
    private final String globalQueueName;
    private final String keepAliveTopicName;

    public DestinationPushStoreCluster(ClientSessionFactory factory, String privateQueueName, String globalQueueName) throws JAXBException
    {
        this.factory = factory;
        
        this.privateQueueName = privateQueueName;
        this.globalQueueName = globalQueueName;
        this.keepAliveTopicName = GLOBAL_INFO_TOPIC_PREFIX+"."+globalQueueName;
        
    }
    
    public synchronized void start() throws ActiveMQException{
        if(this.keepAliveListenerSession == null){
            this.keepAliveListenerSession = factory.createSession(true,true);
            keepAliveListenerSession.createQueue(keepAliveTopicName, RoutingType.MULTICAST, keepAliveTopicName);
            keepAliveListenerSession.createConsumer(keepAliveTopicName).setMessageHandler(this::updateInstancesMap);
            keepAliveListenerSession.start();
            sheduler.scheduleAtFixedRate(this::checkForDeadNodes, CHECK_PERIOD, CHECK_PERIOD, TimeUnit.MILLISECONDS);
            sheduler.scheduleAtFixedRate(this::sendAliveMessage, 0L, CHECK_PERIOD/2, TimeUnit.MILLISECONDS);
        }
    }
    
    public void stop() throws ActiveMQException{
        try
        {
            sheduler.shutdown();
            executor.shutdown();
            sheduler.awaitTermination(CHECK_PERIOD, TimeUnit.MILLISECONDS);
            executor.awaitTermination(CHECK_PERIOD*10, TimeUnit.MILLISECONDS); //had to choose some value
        }
        catch (InterruptedException ex)
        {
            //do nothing - we don't really care.
        }
        finally{
            keepAliveListenerSession.stop();
            keepAliveListenerSession.close();
        }
    }
    
    private void sendAliveMessage(){
        try (ClientSession session = factory.createSession(true, true);
             ClientProducer producer = session.createProducer(keepAliveTopicName))
        {
            ClientMessage keepAliveMessage = session.createMessage(Message.TEXT_TYPE, false, constructValidUntilTime(), System.currentTimeMillis(), (byte)0);
            keepAliveMessage.getBodyBuffer().writeUTF(prepareKeepAliveContent());
            producer.send(keepAliveMessage);
        }
        catch (ActiveMQException ex)
        {
            throw new RuntimeException(ex);
        }
        
    }
    
    private void checkForDeadNodes(){
        instances.entrySet().stream()
                .filter((e) -> e.getValue() < System.currentTimeMillis())
                .map((e) -> e.getKey())
                .forEach(this::sheduleRemovalOfNode); //we shedule this on another executor so that our keepAlive messages won't be affected in case of problems
    }
    
    private void sheduleRemovalOfNode(String deadPrivateQueue){
        ActiveMQRestLogger.LOGGER.info( "Removing node :"+deadPrivateQueue);
        executor.execute(() -> moveMessagesFromDeadPrivateQueyeToPublicQueye(deadPrivateQueue));
        instances.remove(deadPrivateQueue); //we remove now so that we won't check them again.
    }
    
    private void moveMessagesFromDeadPrivateQueyeToPublicQueye(String deadPrivateQueue){
        try(ClientSession session = factory.createSession(true, true);
            ClientConsumer deadPrivateQueyeConsumer = session.createConsumer(deadPrivateQueue);
            ClientProducer globalQueyeProducer = session.createProducer(globalQueueName);)
        {
            session.start();
            ClientMessage msg;
            while((msg = deadPrivateQueyeConsumer.receiveImmediate()) != null)
            {
                globalQueyeProducer.send(msg);
                msg.acknowledge();
            }
        }
        catch (ActiveMQException ex)
        {
            throw new RuntimeException();
        }
    }

    private void updateInstancesMap(ClientMessage message){
        try
        {
            String messageString = message.getBodyBuffer().readUTF();
            InstanceWorkingInfo instanceWorkingInfo = unmarshall(messageString);
            instances.put(instanceWorkingInfo.instanceQueueAddress, instanceWorkingInfo.validUntil);
            message.acknowledge();
        }
        catch (JAXBException | ActiveMQException ex)
        {
            throw new RuntimeException(ex);
        }
    }
    
    private static long constructValidUntilTime()
    {
        return System.currentTimeMillis()+(CHECK_PERIOD*2);
    }

    private String prepareKeepAliveContent()
    {
        try
        {
            InstanceWorkingInfo instanceWorkingInfo = new InstanceWorkingInfo();
            instanceWorkingInfo.instanceQueueAddress = privateQueueName;
            instanceWorkingInfo.validUntil = constructValidUntilTime();
            return marshall(instanceWorkingInfo);
            
        }
        catch (JAXBException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private InstanceWorkingInfo unmarshall(String messageString) throws JAXBException
    {
        return (InstanceWorkingInfo) ctx.createUnmarshaller().unmarshal(new StringReader(messageString));
    }
    
    private String marshall(InstanceWorkingInfo instanceWorkingInfo) throws JAXBException
    {
        StringWriter toReturn = new StringWriter();
        ctx.createMarshaller().marshal(instanceWorkingInfo, toReturn);
        return toReturn.toString();
    }
    
    @XmlRootElement(name = "instance-working")
    @XmlAccessorType(XmlAccessType.FIELD)
    private static class InstanceWorkingInfo{
        
        private String instanceQueueAddress;
        private long validUntil;
        
    }
    
}

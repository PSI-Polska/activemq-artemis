package org.apache.activemq.artemis.rest.queue.push;

import org.apache.activemq.artemis.api.core.ActiveMQAddressExistsException;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistration;
import org.apache.activemq.artemis.rest.queue.push.xml.PushRegistrationUnmarshaler;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID;

public class DestinationPushStore implements PushStore
{

    protected final ClientSessionFactory factory;
    
    protected final PushRegistrationUnmarshaler unmarshaler;

    private static final String REGISTRATION_ID = "reg_id";
    private static final String DESTINATION_ID = "reg_destination";
    protected final String globalQueueName;
    protected final String privateQueueName;

    private DestinationPushStoreCluster cluster;


    public DestinationPushStore(ClientSessionFactory factory, PushRegistrationUnmarshaler unmarshaler, String privateQueueName, String globalQueueName) throws JAXBException, ActiveMQException
    {
        this.globalQueueName = globalQueueName;
        this.factory = factory;
        this.unmarshaler = unmarshaler;
        this.privateQueueName = privateQueueName;

        try(ClientSession session = factory.createSession(false,false)){
            SimpleString globalQueue = new SimpleString(globalQueueName);
            SimpleString instanceQueue = new SimpleString( privateQueueName);

            createDestination(session, instanceQueue);
            createDestination(session, globalQueue);
        }
        
        cluster = new DestinationPushStoreCluster( factory, privateQueueName, globalQueueName);
        cluster.start();
    }

    @Override
    protected void finalize() throws Throwable
    {
        cluster.stop();
    }
    
    

    private void createDestination(final ClientSession session,SimpleString queueName) throws ActiveMQException
    {
        
        try
        {
            session.createQueue(queueName, RoutingType.ANYCAST, queueName, true);
        }
        catch (ActiveMQQueueExistsException | ActiveMQAddressExistsException ex)
        {
            //do nothing - it is better to discard the exception than check for queue existence as it poses less risk for race conditions
        }
    }

    @Override
    public List<PushRegistration> getRegistrations() throws ActiveMQException, JAXBException {
        try (ClientSession session = factory.createSession(false,false);
             ClientConsumer consumer = session.createConsumer( privateQueueName, true)) {
            session.start();
            List<PushRegistration> toReturn = new ArrayList<>();
            ClientMessage message = consumer.receiveImmediate();
            while(message != null){
                toReturn.add(unmarshaler.unmarshallPushRegistration(message.getBodyBuffer().readUTF()));
                message = consumer.receiveImmediate();
            }
            session.commit();
            return toReturn;
        }
    }

    @Override
    public void add(PushRegistration reg) throws ActiveMQException, JAXBException {
            
        try (ClientSession session = factory.createSession(true,false);
             ClientProducer producer = session.createProducer( privateQueueName)) {
            ClientMessage messageToSend = prepareMessageWithPushRegistration(session, reg);
            producer.send(messageToSend);
        }
    }

    public void addGlobally(PushRegistration reg) throws ActiveMQException, JAXBException {

        try (ClientSession session = factory.createSession(true,false);
             ClientProducer producer = session.createProducer(globalQueueName)) {
            ClientMessage messageToSend = prepareMessageWithPushRegistration(session, reg);
            producer.send(messageToSend);
        }
    }

    protected ClientMessage prepareMessageWithPushRegistration(
                                                             final ClientSession session,
                                                             PushRegistration reg) throws JAXBException
    {
        ClientMessage messageToSend = session.createMessage(Message.TEXT_TYPE, true, Long.MAX_VALUE, System.currentTimeMillis(), (byte)0);
        messageToSend.getBodyBuffer().writeUTF(unmarshaler.marshallPushRegistration(reg));
        messageToSend.putStringProperty(HDR_DUPLICATE_DETECTION_ID, UUID.randomUUID().toString());
        messageToSend.putStringProperty(DESTINATION_ID, reg.getDestination());
        messageToSend.putStringProperty(REGISTRATION_ID, reg.getId());
        return messageToSend;
    }

    @Override
    public void remove(PushRegistration reg) throws ActiveMQException {
        try (ClientSession session = factory.createSession(false,true))
        {
            ClientConsumer consumer = session.createConsumer( privateQueueName, String.format( "%s = '%s'", REGISTRATION_ID, reg.getId()));
            session.start();
            ClientMessage oldRegistration = consumer.receiveImmediate();
            if(oldRegistration != null){
                oldRegistration.acknowledge();
            }
        }
    }

    @Override
    public List<PushRegistration> getByDestination(String destination) throws ActiveMQException, JAXBException {
        try (
                ClientSession session = factory.createSession(false,false);
                ClientConsumer consumer = session.createConsumer( privateQueueName, String.format( "%s = '%s'", DESTINATION_ID, destination), true)
                )
        {
            session.start();
            List<PushRegistration> toReturn = new ArrayList<>();
            ClientMessage message = consumer.receiveImmediate();
            while(message != null){
                toReturn.add(unmarshaler.unmarshallPushRegistration(message.getBodyBuffer().readUTF()));
                message = consumer.receiveImmediate();
            }
            session.commit();
            return toReturn;
        }
    }

    @Override
    public void update(PushRegistration reg) throws ActiveMQException, JAXBException {
        try (
                ClientSession session = factory.createSession(false,false);
                ClientConsumer consumer = session.createConsumer( privateQueueName, String.format( "%s = '%s'", REGISTRATION_ID, reg.getId()))
                )
        {
            session.start();
            ClientMessage oldRegistration = consumer.receiveImmediate();
            ClientProducer producer = session.createProducer( privateQueueName);
            producer.send(prepareMessageWithPushRegistration(session, reg));
            if(oldRegistration != null){
                oldRegistration.acknowledge();
            }
            session.commit();
        }
    }

    @Override
    @SuppressWarnings("empty-statement")
    public void removeAll() throws ActiveMQException {
        try (
                ClientSession session = factory.createSession(false,true)
            )
        {
            session.deleteQueue( privateQueueName);
        }
    }

    @Override
    public long count() throws ActiveMQException {
        try (
                ClientSession session = factory.createSession(false,true);
                ClientConsumer consumer = session.createConsumer( privateQueueName, true)
        )
        {
            session.start();
            long counter = 0L;
            ClientMessage message = consumer.receiveImmediate();
            while (message != null) {
                counter++;
                message = consumer.receiveImmediate();
            }
            session.commit();

            return counter;
        }
    }

}

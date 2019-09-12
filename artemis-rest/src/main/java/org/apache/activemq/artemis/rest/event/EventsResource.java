
package org.apache.activemq.artemis.rest.event;

import de.psi.pjf.bus.registry.client.RegistryClient;
import de.psi.pjf.bus.registry.client.RegistryResponse;
import de.psi.pjf.bus.registry.client.jaxrs.JaxrsRegistryClient;
import de.psi.pjf.bus.registry.client.jaxrs.cache.CacheFactory;
import de.psi.pjf.bus.registry.common.event.EventRoute;
import de.psi.pjf.bus.registry.common.event.EventRoutes;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.MessageServiceManager;
import org.apache.activemq.artemis.rest.util.Constants;
import org.apache.activemq.artemis.rest.util.HttpMessageHelper;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.jboss.resteasy.specimpl.ResteasyHttpHeaders;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Path( Constants.PATH_FOR_EVENTS )
public class EventsResource
{

    private MessageServiceManager messageServiceManager;
    private RegistryClient registryClient;

    public EventsResource( MessageServiceManager aMessageServiceManager )
    {
        messageServiceManager = aMessageServiceManager;
        String registryAddressesProperty = System.getProperty( "de.psi.pjf.bus.registry.addresses", "" );
        String[] addresses = registryAddressesProperty.split( "," );
        Collection< URI > registryAddresses = Arrays.stream( addresses )
            .map( String::trim )
            .map( URI::create )
            .collect( Collectors.toList() );
        registryClient = initRegistryClient( registryAddresses );
    }

    @POST
    @Path( "/{destinationName}" )
    public Response create( @Context HttpHeaders headers,
                            @Context UriInfo uriInfo,
                            @PathParam( "destinationName" ) String destinationName,
                            byte[] body) {
        ActiveMQRestLogger.LOGGER.info( "Handling POST request for \"" + uriInfo.getRequestUri() + "\"");

        try
        {
            internalHandleEvent( destinationName, headers, body );
        }catch( NotFoundException e){
            Response error = Response.status(
                Response.Status.NOT_FOUND).entity( "Event destination not known for: "+destinationName).type( "text/plain").build();
            throw new WebApplicationException( e, error);
        }
        catch(Exception e){
            ActiveMQRestLogger.LOGGER.error( "Error handling event", e );
            Response error = Response.serverError().entity("Problem processing an event: " + e.getCause()).type("text/plain").build();
            throw new WebApplicationException( e, error);
        }
        Response.ResponseBuilder builder = Response.status(201);
        return builder.build();
    }

    private void internalHandleEvent(String destinationName,HttpHeaders headers,byte[] body) throws ActiveMQException
    {
        ActiveMQRestLogger.LOGGER.info(String.format("Processing event %s",destinationName ));

        RegistryResponse< EventRoutes > response = registryClient.resolveEvent( destinationName );
        EventRoutes eventRoutes = response.get();

        try(ClientSession session = messageServiceManager.getConsumerSessionFactory().createSession())
        {
            session.start();
            eventRoutes.getRoutes().forEach(
                aEventRoute -> sendToQueue( aEventRoute, headers, body, session ) );
        }
    }


    private void sendToQueue( EventRoute aEventRoute, HttpHeaders headers, byte[] body, ClientSession aSession)
    {
        try
        {
            ClientProducer producer = aSession.createProducer( aEventRoute.getDestinationName() );
            HttpHeaders augmentedHeaders = addContentType( headers,aEventRoute );
            ClientMessage message = createActiveMQMessage( augmentedHeaders, body, aSession );
            producer.send( message );
        }catch(Exception e){
            throw new RuntimeException( "Error sending message", e );
        }
    }

    private ClientMessage createActiveMQMessage( HttpHeaders headers,
                                                   byte[] body,
                                                   ClientSession session) throws Exception {
        ClientMessage message = session.createMessage( Message.BYTES_TYPE, true);

        UUID uid = UUIDGenerator.getInstance().generateUUID();
        message.setUserID(uid);
        HttpMessageHelper.writeHttpMessage( headers, body, message);
        return message;
    }

    private RegistryClient initRegistryClient( Collection< URI >  aRegistryAddresses ){
        ClientBuilder clientBuilder = ClientBuilder.newBuilder();

        CacheFactory cacheFactory = CacheFactory.builder()
            .expirationTime( 12L )
            .expirationTimeUnit( TimeUnit.HOURS )
            .build();

        return JaxrsRegistryClient.builder()
            .registryAddresses( aRegistryAddresses )
            .clientBuilder( clientBuilder )
            .cacheFactory( cacheFactory )
            .executorService( Executors.newCachedThreadPool() )
            .build();
    }

    private HttpHeaders addContentType(HttpHeaders aHttpHeaders, EventRoute aEventRoute){
        MultivaluedMap hdrs = aHttpHeaders.getRequestHeaders();
        hdrs.putSingle( HttpHeaders.CONTENT_TYPE, chooseContentType( aHttpHeaders.getMediaType(), aEventRoute.getContentTypes() ).toString() );
        return new ResteasyHttpHeaders( hdrs );
    }

    private MediaType chooseContentType( MediaType userProvided, Collection< String > aContentTypes )
    {
        return aContentTypes.stream()
            .map( MediaType::valueOf )
            .filter( userProvided::equals )
            .findAny()
            .orElseThrow( () -> new RuntimeException( "Request content-type mismatch." ) );

    }

}


package org.apache.activemq.artemis.rest.event;

import org.apache.activemq.artemis.rest.ActiveMQRestLogger;
import org.apache.activemq.artemis.rest.util.Constants;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;


@Path( Constants.PATH_FOR_EVENTS )
public class EventsResource
{
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
        }catch(DestinationMissingException e){
            Response error = Response.serverError().entity("Event destination not known for: "+destinationName).type("text/plain").build();
            throw new WebApplicationException( e, error);
        }
        catch(Exception e){
            Response error = Response.serverError().entity("Problem processing an event: " + e.getMessage()).type("text/plain").build();
            throw new WebApplicationException( e, error);
        }
        Response.ResponseBuilder builder = Response.status(201);
        return builder.build();
    }

    private void internalHandleEvent(String destinationNamem,HttpHeaders headers,byte[] body) throws DestinationMissingException{
        ActiveMQRestLogger.LOGGER.info(String.format("Processing event %s",destinationNamem ));
    }
}

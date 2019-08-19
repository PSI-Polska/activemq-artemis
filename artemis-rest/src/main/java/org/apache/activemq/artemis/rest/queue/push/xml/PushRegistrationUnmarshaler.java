// ******************************************************************
//
//  PushRegistrationUnmarshaler.java
//  Copyright 2019 PSI AG. All rights reserved.
//  PSI PROPRIETARY/CONFIDENTIAL. Use is subject to license terms
//
// ******************************************************************

package org.apache.activemq.artemis.rest.queue.push.xml;

import org.apache.activemq.artemis.rest.topic.PushTopicRegistration;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.StringReader;
import java.io.StringWriter;


public class PushRegistrationUnmarshaler
{
    protected  JAXBContext ctx;

    public PushRegistrationUnmarshaler()
    {
        try
        {
            ctx = JAXBContext.newInstance( PushRegistration.class, PushTopicRegistration.class);
        }
        catch( JAXBException aE )
        {
            throw new RuntimeException( "JAXB context failed.", aE );
        }
    }

    public String marshallPushRegistration(PushRegistration reg) throws JAXBException{
        StringWriter toReturn = new StringWriter();
        ctx.createMarshaller().marshal(reg, toReturn);
        return toReturn.toString();
    }

    public PushRegistration unmarshallPushRegistration(String msg) throws JAXBException{
        return (PushRegistration) ctx.createUnmarshaller().unmarshal(new StringReader( msg));
    }
}

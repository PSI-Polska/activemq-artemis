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
package org.apache.activemq.artemis.rest.push.balancer;

import javax.xml.bind.annotation.*;

@XmlRootElement(name = "push-info")
@XmlAccessorType(XmlAccessType.PROPERTY)
public class PushInfo {

    private String instanceId;
    private long pushConsumers;
    private long pushSubscriptions;

    public PushInfo() {
        this(null, 0L, 0L);
    }

    public PushInfo(String instanceId, long pushConsumers, long pushSubscriptions) {
        this.instanceId = instanceId;
        this.pushConsumers = pushConsumers;
        this.pushSubscriptions = pushSubscriptions;
    }

    @XmlAttribute(name = "instance-id")
    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    @XmlAttribute(name = "push-consumers")
    public long getPushConsumers() {
        return pushConsumers;
    }

    public void setPushConsumers(long queueSubscriptions) {
        this.pushConsumers = queueSubscriptions;
    }

    @XmlAttribute(name = "push-subscriptions")
    public long getPushSubscriptions() {
        return pushSubscriptions;
    }

    public void setPushSubscriptions(long topicSubscriptions) {
        this.pushSubscriptions = topicSubscriptions;
    }
}

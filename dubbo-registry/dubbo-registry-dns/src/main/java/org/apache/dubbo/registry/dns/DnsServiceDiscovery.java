/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.registry.dns;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.CompositeConfiguration;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.event.EventDispatcher;
import org.apache.dubbo.event.EventListener;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.timer.BiPollingTimeTask;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD;
import static org.apache.dubbo.registry.Constants.DEFAULT_SESSION_TIMEOUT;
import static org.apache.dubbo.registry.Constants.REGISTRY_RECONNECT_PERIOD_KEY;
import static org.apache.dubbo.registry.Constants.SESSION_TIMEOUT_KEY;
import static org.apache.dubbo.registry.dns.DnsConstants.DNS_SERVER;

/**
 * 2019-11-08
 */
public class DnsServiceDiscovery implements ServiceDiscovery, EventListener<ServiceInstancesChangedEvent> {

    private final static Logger logger = LoggerFactory.getLogger(DnsServiceDiscovery.class);

    final Map<String, Integer> subscribeServices = new ConcurrentHashMap<>();
    HashedWheelTimer periodTimer;
    long dnsPollingPeriod;

    private DnsLookup dnsLookup;

    protected String defaultDnsUrlPostfix;
    protected int defaultPort;
    protected String dnsServer;
    EventDispatcher dispatcher;

    public DnsServiceDiscovery() {

    }

    @Override
    public void onEvent(ServiceInstancesChangedEvent event) {

    }

    @Override
    public void initialize(URL registryURL) throws Exception {
        int sessionTimeout = registryURL.getParameter(SESSION_TIMEOUT_KEY, DEFAULT_SESSION_TIMEOUT);

        this.dispatcher = EventDispatcher.getDefaultExtension();
        this.dispatcher.addEventListener(this);

        CompositeConfiguration compositeConfiguration = ApplicationModel.getEnvironment().getConfiguration("dubbo.registry", "dns");
        String ns = compositeConfiguration.getString("ns", "default");
        String zone = compositeConfiguration.getString("zone", "cluster.local");
        this.defaultDnsUrlPostfix = "." + ns + ".svc." + zone;
        // 默认使用dubbo协议端口
        this.defaultPort = ApplicationModel.getEnvironment().getConfiguration("dubbo.registry", "dns").getInteger("port",
                ExtensionLoader.getExtensionLoader(Protocol.class).getExtension("dubbo").getDefaultPort());
        this.dnsLookup = new DnsLookup();
        this.dnsServer = ApplicationModel.getEnvironment().getConfiguration("dubbo.registry", "dns").getString(DNS_SERVER);
        this.dnsPollingPeriod = registryURL.getParameter(REGISTRY_RECONNECT_PERIOD_KEY, DEFAULT_REGISTRY_RECONNECT_PERIOD / 3);
        periodTimer = new HashedWheelTimer(new NamedThreadFactory("DubboDnsCycleTimer", true), dnsPollingPeriod / 5, TimeUnit.MILLISECONDS, 128);
    }

    @Override
    public void destroy() {

    }

    @Override
    public void register(ServiceInstance serviceInstance) throws RuntimeException {

    }

    @Override
    public void update(ServiceInstance serviceInstance) throws RuntimeException {

    }

    @Override
    public void unregister(ServiceInstance serviceInstance) throws RuntimeException {

    }

    @Override
    public Set<String> getServices() {
        return Collections.EMPTY_SET;
    }

    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
        // polling the dns server.
        periodTimer.newTimeout(new DNSPollingTask(this.dnsPollingPeriod, listener.getServiceName(), this), this.dnsPollingPeriod, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) {
        try {
            List<String> list = dnsLookup.nsLookupForA(getDNSURL(serviceName));
            if (CollectionUtils.isEmpty(list)) {
                subscribeServices.remove(serviceName);
                return Collections.EMPTY_LIST;
            }
            // sort (for compute hashcode)
            Collections.sort(list);
            List<ServiceInstance> serviceInstances = new ArrayList<>(list.size());
            for (String ip : list) {
                serviceInstances.add(new DefaultServiceInstance(String.valueOf(System.nanoTime()), serviceName, ip, defaultPort));
            }
            subscribeServices.put(serviceName, System.identityHashCode(serviceInstances));
            return serviceInstances;
        } catch (InterruptedException e) {

        }
        return Collections.EMPTY_LIST;
    }


    protected String getDNSURL(String serviceName) {
        if (StringUtils.isNoneEmpty(dnsServer)) {
            return dnsServer;
        }
        return serviceName + this.defaultDnsUrlPostfix;
    }

    static class DNSPollingTask extends BiPollingTimeTask<String, DnsServiceDiscovery> {

        public DNSPollingTask(Long tick, String serviceName, DnsServiceDiscovery dnsServiceDiscovery) {
            super(tick, serviceName, dnsServiceDiscovery);
        }

        @Override
        protected void doTask() {
            Integer hashcode = u.subscribeServices.get(t);
            List<ServiceInstance> newServiceInstances = u.getInstances(t);
            if (hashcode == null && CollectionUtils.isEmpty(newServiceInstances)) {
                return;
            } else if (hashcode == null) {
                u.dispatchServiceInstancesChangedEvent(t, newServiceInstances);
                u.subscribeServices.put(t, System.identityHashCode(newServiceInstances));
                return;
            }
            int newHashcode = System.identityHashCode(newServiceInstances);
            if (!hashcode.equals(newHashcode)) {
                u.dispatchServiceInstancesChangedEvent(t, newServiceInstances);
                u.subscribeServices.put(t, System.identityHashCode(newServiceInstances));
            }
        }
    }
}

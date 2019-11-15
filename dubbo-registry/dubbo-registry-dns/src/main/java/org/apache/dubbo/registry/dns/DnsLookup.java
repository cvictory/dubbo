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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRawRecord;
import io.netty.handler.codec.dns.DnsRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.resolver.ResolvedAddressTypes;
import io.netty.resolver.dns.DnsNameResolver;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.Future;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 2019-11-04
 */
public class DnsLookup {
    private static NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);

    public List<String> nsLookupForA(String host) throws InterruptedException {
//        return Arrays.asList("30.5.124.3");
        return nsLookup(host, DnsRecordType.A.name());
    }

    public List<String> nsLookup(String host, String recordType) throws InterruptedException {
        DnsNameResolver resolver = nameResolverBuilder(recordType).build();
        Map<String, Future<List<DnsRecord>>> futures =
                new LinkedHashMap<String, Future<List<DnsRecord>>>();

        for (DnsQuestion question : newQuestions(host, recordType)) {
            futures.put(question.name(), resolver.resolveAll(question));
        }
        for (Map.Entry<String, Future<List<DnsRecord>>> e : futures.entrySet()) {
            String hostname = e.getKey();
            List<DnsRecord> records = e.getValue().sync().getNow();

            if (records == null || records.isEmpty()) {
                continue;
            }

            final boolean hasLoopbackARecords =
                    records.stream()
                            .filter(r -> r instanceof DnsRawRecord)
                            .map(DnsRawRecord.class::cast)
                            .anyMatch(r -> r.type() == DnsRecordType.A &&
                                    r.content().getByte(r.content().readerIndex()) == 127);


            List<String> result = new ArrayList<>();
            for (DnsRecord r : records) {
                String ipAddr = unmarshalIpAddr(r, hasLoopbackARecords);
                if (ipAddr == null) {
                    continue;
                }
                result.add(ipAddr);
            }
            return result;
        }
        return null;
    }

    private static List<DnsQuestion> newQuestions(
            String hostname, String recordType) {
        List<DnsRecordType> list;
        if (DnsRecordType.A.name().equalsIgnoreCase(recordType)) {
            list = Arrays.asList(DnsRecordType.A);
        } else if (DnsRecordType.AAAA.name().equalsIgnoreCase(recordType)) {
            list = Arrays.asList(DnsRecordType.AAAA);
        } else if (DnsRecordType.MX.name().equalsIgnoreCase(recordType)) {
            list = Arrays.asList(DnsRecordType.MX);
        } else {
            list = new ArrayList<>();
            list.add(DnsRecordType.A);
            list.add(DnsRecordType.AAAA);
        }
        final List result = new ArrayList(list.size());
        for (DnsRecordType dnsRecordType : list) {
            result.add(new DefaultDnsQuestion(hostname, dnsRecordType));
        }
        return result;
    }


    private DnsNameResolverBuilder nameResolverBuilder(String recordType) {
        DnsNameResolverBuilder resolverBuilder = new DnsNameResolverBuilder(bossGroup.next())
                .channelType(NioDatagramChannel.class)
//                .ttl(30, 120)
//                .traceEnabled(true)
                .maxQueriesPerResolve(1)
                .optResourceEnabled(false);
        if (!DnsRecordType.MX.name().equalsIgnoreCase(recordType)) {
            resolverBuilder.resolvedAddressTypes(ResolvedAddressTypes.IPV4_PREFERRED);
        }
        return resolverBuilder;
    }


    private String unmarshalIpAddr(DnsRecord r, boolean hasLoopbackARecords) {
        final DnsRecordType type = r.type();
        final ByteBuf content = ((ByteBufHolder) r).content();
        final int contentLen = content.readableBytes();
        // Convert the content into an IP address and then into an endpoint.
        final String ipAddr;
        final byte[] addrBytes = new byte[contentLen];
        content.getBytes(content.readerIndex(), addrBytes);

        if (contentLen == 16) {
            // Convert some IPv6 addresses into IPv4 addresses to remove duplicate endpoints.
            if (addrBytes[0] == 0x00 && addrBytes[1] == 0x00 &&
                    addrBytes[2] == 0x00 && addrBytes[3] == 0x00 &&
                    addrBytes[4] == 0x00 && addrBytes[5] == 0x00 &&
                    addrBytes[6] == 0x00 && addrBytes[7] == 0x00 &&
                    addrBytes[8] == 0x00 && addrBytes[9] == 0x00) {

                if (addrBytes[10] == 0x00 && addrBytes[11] == 0x00) {
                    if (addrBytes[12] == 0x00 && addrBytes[13] == 0x00 &&
                            addrBytes[14] == 0x00 && addrBytes[15] == 0x01) {
                        // Loopback address (::1)
                        if (hasLoopbackARecords) {
                            // Contains an IPv4 loopback address already; skip.
                            return null;
                        } else {
                            ipAddr = "::1";
                        }
                    } else {
                        // IPv4-compatible address.
                        ipAddr = NetUtil.bytesToIpAddress(addrBytes, 12, 4);
                    }
                } else if (addrBytes[10] == -1 && addrBytes[11] == -1) {
                    // IPv4-mapped address.
                    ipAddr = NetUtil.bytesToIpAddress(addrBytes, 12, 4);
                } else {
                    ipAddr = NetUtil.bytesToIpAddress(addrBytes);
                }
            } else {
                ipAddr = NetUtil.bytesToIpAddress(addrBytes);
            }
        } else {
            ipAddr = NetUtil.bytesToIpAddress(addrBytes);
        }
        return ipAddr;
    }
}

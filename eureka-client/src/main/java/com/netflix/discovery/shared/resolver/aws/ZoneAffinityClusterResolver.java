/*
 * Copyright 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.discovery.shared.resolver.aws;

import java.util.Collections;
import java.util.List;

import com.netflix.discovery.shared.resolver.ClusterResolver;
import com.netflix.discovery.shared.resolver.ResolverUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * It is a cluster resolver that reorders the server list, such that the first server on the list
 * is in the same zone as the client. The server is chosen randomly from the available pool of server in
 * that zone. The remaining servers are appended in a random order, local zone first, followed by servers from other zones.
 *
 * @author Tomasz Bak
 */
public class ZoneAffinityClusterResolver implements ClusterResolver<AwsEndpoint> {

    private static final Logger logger = LoggerFactory.getLogger(ZoneAffinityClusterResolver.class);

    private final ClusterResolver<AwsEndpoint> delegate;
    private final String myZone;
    private final boolean zoneAffinity;

    /**
     * A zoneAffinity defines zone affinity (true) or anti-affinity rules (false).
     */
    public ZoneAffinityClusterResolver(ClusterResolver<AwsEndpoint> delegate, String myZone, boolean zoneAffinity) {
        this.delegate = delegate;// ConfigClusterResolver类型
        this.myZone = myZone;
        this.zoneAffinity = zoneAffinity;// 默认true
    }

    @Override
    public String getRegion() {
        return delegate.getRegion();
    }

    @Override
    public List<AwsEndpoint> getClusterEndpoints() {
        // 第一步调用delegate.getClusterEndpoints()，这里的delegate为ConfigClusterResolver解析配置的region以及zone，以及zone对应的url。
        // 把所有的url封装成一个个的AwsEndpoint
        // region:beijing、zone:tongzhou,haidian
        // 第二步根据当前服务所在的zone,把AwsEndpoint列表分成两个组
        List<AwsEndpoint>[] parts = ResolverUtils.splitByZone(delegate.getClusterEndpoints(), myZone);
        // 与当前服务在同zone的服务列表
        List<AwsEndpoint> myZoneEndpoints = parts[0];
        // 与当前服务不在同zone的服务列表
        List<AwsEndpoint> remainingEndpoints = parts[1];
        // 对myZoneEndpoints和remainingEndpoints根据当前服务IP随机打乱，之后merge到一起
        List<AwsEndpoint> randomizedList = randomizeAndMerge(myZoneEndpoints, remainingEndpoints);
        // 排序，默认不排
        if (!zoneAffinity) {
            Collections.reverse(randomizedList);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Local zone={}; resolved to: {}", myZone, randomizedList);
        }

        return randomizedList;
    }

    private static List<AwsEndpoint> randomizeAndMerge(List<AwsEndpoint> myZoneEndpoints, List<AwsEndpoint> remainingEndpoints) {
        if (myZoneEndpoints.isEmpty()) {
            return ResolverUtils.randomize(remainingEndpoints);
        }
        if (remainingEndpoints.isEmpty()) {
            return ResolverUtils.randomize(myZoneEndpoints);
        }
        // 根据当前服务IP打乱myZoneEndpoints
        List<AwsEndpoint> mergedList = ResolverUtils.randomize(myZoneEndpoints);
        // 根据当前服务IP打乱remainingEndpoints之后加入到mergedList中
        mergedList.addAll(ResolverUtils.randomize(remainingEndpoints));
        return mergedList;
    }
}

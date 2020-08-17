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

package com.netflix.eureka.util.batcher;

import com.netflix.eureka.util.batcher.TaskProcessor.ProcessingResult;

/**
 * {@link TrafficShaper} provides admission control policy prior to dispatching tasks to workers.
 * It reacts to events coming via reprocess requests (transient failures, congestion), and delays the processing
 * depending on this feedback.
 *
 * @author Tomasz Bak
 */
class TrafficShaper {

    /**
     * Upper bound on delay provided by configuration.
     */
    private static final long MAX_DELAY = 30 * 1000;

    private final long congestionRetryDelayMs;
    private final long networkFailureRetryMs;

    private volatile long lastCongestionError;
    private volatile long lastNetworkFailure;

    // congestionRetryDelayMs 1000
    // networkFailureRetryMs 100
    TrafficShaper(long congestionRetryDelayMs, long networkFailureRetryMs) {
        this.congestionRetryDelayMs = Math.min(MAX_DELAY, congestionRetryDelayMs);
        this.networkFailureRetryMs = Math.min(MAX_DELAY, networkFailureRetryMs);
    }

    void registerFailure(ProcessingResult processingResult) {
        // 网络拥堵，记录lastCongestionError为当前时间
        if (processingResult == ProcessingResult.Congestion) {
            lastCongestionError = System.currentTimeMillis();
        // 失败，记录lastNetworkFailure为当前时间
        } else if (processingResult == ProcessingResult.TransientError) {
            lastNetworkFailure = System.currentTimeMillis();
        }
    }

    long transmissionDelay() {
        if (lastCongestionError == -1 && lastNetworkFailure == -1) {
            return 0;
        }

        long now = System.currentTimeMillis();
        if (lastCongestionError != -1) {
            // 记录当前时间和上次网络拥堵的差值
            long congestionDelay = now - lastCongestionError;
            // 这个if就是在上次网络拥堵的情况下，判断当前时间距离上次时间的差值然后还需要再等待多久
            if (congestionDelay >= 0 && congestionDelay < congestionRetryDelayMs) {
                return congestionRetryDelayMs - congestionDelay;
            }
            lastCongestionError = -1;
        }

        if (lastNetworkFailure != -1) {
            long failureDelay = now - lastNetworkFailure;
            // 参考lastCongestionError计算
            if (failureDelay >= 0 && failureDelay < networkFailureRetryMs) {
                return networkFailureRetryMs - failureDelay;
            }
            lastNetworkFailure = -1;
        }
        return 0;
    }
}

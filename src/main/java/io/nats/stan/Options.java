/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.time.Duration;

class Options {
    private String natsUrl;
    private io.nats.client.Connection natsConn;
    private Duration connectTimeout;
    private Duration ackTimeout;
    private String discoverPrefix;
    private int maxPubAcksInFlight;

    private Options(Builder builder) {
        this.natsUrl = builder.natsUrl;
        this.natsConn = builder.natsConn;
        this.connectTimeout = builder.connectTimeout;
        this.ackTimeout = builder.ackTimeout;
        this.discoverPrefix = builder.discoverPrefix;
        this.maxPubAcksInFlight = builder.maxPubAcksInFlight;
    }

    String getNatsUrl() {
        return natsUrl;
    }

    io.nats.client.Connection getNatsConn() {
        return natsConn;
    }

    Duration getConnectTimeout() {
        return connectTimeout;
    }

    Duration getAckTimeout() {
        return ackTimeout;
    }

    String getDiscoverPrefix() {
        return discoverPrefix;
    }

    int getMaxPubAcksInFlight() {
        return maxPubAcksInFlight;
    }

    static final class Builder {
        private String natsUrl = ConnectionImpl.DEFAULT_NATS_URL;
        private io.nats.client.Connection natsConn;
        private Duration connectTimeout = Duration.ofSeconds(ConnectionImpl.DEFAULT_CONNECT_WAIT);
        private Duration ackTimeout = Duration.ofMillis(SubscriptionImpl.DEFAULT_ACK_WAIT);
        private String discoverPrefix = ConnectionImpl.DEFAULT_DISCOVER_PREFIX;
        private int maxPubAcksInFlight = ConnectionImpl.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;

        public Builder setAckTimeout(Duration ackTimeout) {
            this.ackTimeout = ackTimeout;
            return this;
        }

        public Builder setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder setDiscoverPrefix(String discoverPrefix) {
            this.discoverPrefix = discoverPrefix;
            return this;
        }

        public Builder setMaxPubAcksInFlight(int maxPubAcksInFlight) {
            this.maxPubAcksInFlight = maxPubAcksInFlight;
            return this;
        }

        public Builder setNatsConn(io.nats.client.Connection natsConn) {
            this.natsConn = natsConn;
            return this;
        }

        public Builder setNatsUrl(String natsUrl) {
            this.natsUrl = natsUrl;
            return this;
        }

        public Options create() {
            return new Options(this);
        }
    }
}

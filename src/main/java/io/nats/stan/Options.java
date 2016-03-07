/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.time.Duration;

public class Options {
  private String natsUrl;
  private io.nats.client.Connection natsConn;
  private Duration connectTimeout; // milliseconds
  private Duration ackTimeout; // milliseconds
  private String discoverPrefix;
  private int maxPubAcksInFlight;
  private String clientId;
  private String clusterId;

  Options(Builder builder) {
    this.natsUrl = builder.natsUrl;
    this.natsConn = builder.natsConn;
    this.connectTimeout = builder.connectTimeout;
    this.ackTimeout = builder.ackTimeout;
    this.discoverPrefix = builder.discoverPrefix;
    this.maxPubAcksInFlight = builder.maxPubAcksInFlight;
    this.clientId = builder.clientId;
    this.clusterId = builder.clusterId;
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

  String getClientId() {
    return clientId;
  }

  String getClusterId() {
    return clusterId;
  }

  public static final class Builder {
    private String natsUrl = ConnectionImpl.DEFAULT_NATS_URL;
    private io.nats.client.Connection natsConn;
    private Duration connectTimeout; // milliseconds
    private Duration ackTimeout; // milliseconds
    private String discoverPrefix;
    private int maxPubAcksInFlight;
    private String clientId;
    private String clusterId;

    public Builder setAckTimeout(Duration ackTimeout) {
      this.ackTimeout = ackTimeout;
      return this;
    }

    public Builder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setClusterId(String clusterId) {
      this.clusterId = clusterId;
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

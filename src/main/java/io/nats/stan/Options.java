/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.time.Duration;

public class Options {
  private String natsURL = ConnectionImpl.DEFAULT_NATS_URL;
  private io.nats.client.Connection natsConn;
  private Duration connectTimeout; // milliseconds
  private Duration ackTimeout; // milliseconds
  private String discoverPrefix;
  private int maxPubAcksInFlight;
  private String clientID;
  private String clusterID;

  String getNatsURL() {
    return natsURL;
  }

  void setNatsURL(String natsURL) {
    this.natsURL = natsURL;
  }

  io.nats.client.Connection getNatsConn() {
    return natsConn;
  }

  void setNatsConn(io.nats.client.Connection natsConn) {
    this.natsConn = natsConn;
  }

  Duration getConnectTimeout() {
    return connectTimeout;
  }

  void setConnectTimeout(Duration connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  Duration getAckTimeout() {
    return ackTimeout;
  }

  void setAckTimeout(Duration ackTimeout) {
    this.ackTimeout = ackTimeout;
  }

  String getDiscoverPrefix() {
    return discoverPrefix;
  }

  void setDiscoverPrefix(String discoverPrefix) {
    this.discoverPrefix = discoverPrefix;
  }

  int getMaxPubAcksInFlight() {
    return maxPubAcksInFlight;
  }

  void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
    this.maxPubAcksInFlight = maxPubAcksInFlight;
  }

  String getClientID() {
    return clientID;
  }

  void setClientID(String clientID) {
    this.clientID = clientID;
  }

  String getClusterID() {
    return clusterID;
  }

  void setClusterID(String clusterID) {
    this.clusterID = clusterID;
  }
}

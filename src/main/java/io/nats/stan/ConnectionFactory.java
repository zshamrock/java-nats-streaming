/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//
public class ConnectionFactory {
  Duration ackTimeout = Duration.ofMillis(SubscriptionImpl.DEFAULT_ACK_WAIT);
  Duration connectTimeout = Duration.ofSeconds(ConnectionImpl.DEFAULT_CONNECT_WAIT);
  String discoverPrefix = ConnectionImpl.DEFAULT_DISCOVER_PREFIX;
  int maxPubAcksInFlight = ConnectionImpl.DEFAULT_MAX_PUB_ACKS_IN_FLIGHT;
  String natsURL = ConnectionImpl.DEFAULT_NATS_URL;
  String clientID;
  String clusterID;

  ConnectionFactory() {}

  ConnectionFactory(String clusterId, String clientId) {
    this.clusterID = clusterId;
    this.clientID = clientId;
  }

  ConnectionImpl createConnection() throws IOException, TimeoutException {
    ConnectionImpl conn = new ConnectionImpl(options());
    conn.connect();
    return conn;
  }

  Options options() {
    Options opts = new Options();
    opts.setConnectTimeout(connectTimeout);
    opts.setAckTimeout(ackTimeout);
    opts.setDiscoverPrefix(discoverPrefix);
    opts.setMaxPubAcksInFlight(maxPubAcksInFlight);
    opts.setNatsURL(natsURL);
    opts.setClientID(clientID);
    opts.setClusterID(clusterID);
    return opts;
  }

  /**
   * Returns the ACK timeout.
   * 
   * @return the ackTimeout
   */
  public Duration getAckTimeout() {
    return ackTimeout;
  }

  /**
   * Sets the ACK timeout duration.
   * 
   * @param ackTimeout the ackTimeout to set
   */
  public void setAckTimeout(Duration ackTimeout) {
    this.ackTimeout = ackTimeout;
  }

  /**
   * Sets the ACK timeout in the specified time unit.
   * 
   * @param ackTimeout the ackTimeout to set
   * @param unit the time unit to set
   */
  public void setAckTimeout(long ackTimeout, TimeUnit unit) {
    this.ackTimeout = Duration.ofMillis(unit.toMillis(ackTimeout));
  }

  /**
   * Returns the connect timeout interval in milliseconds.
   * 
   * @return the connectTimeout
   */
  public Duration getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Sets the connect timeout duration.
   * 
   * @param connectTimeout the connectTimeout to set
   */
  public void setConnectTimeout(Duration connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  /**
   * Sets the connect timeout in the specified time unit.
   * 
   * @param connectTimeout the connectTimeout to set
   * @param unit the time unit to set
   */
  public void setConnectTimeout(long connectTimeout, TimeUnit unit) {
    this.connectTimeout = Duration.ofMillis(unit.toMillis(connectTimeout));
  }


  /**
   * Returns the currently configured discover prefix string.
   * 
   * @return the discoverPrefix
   */
  public String getDiscoverPrefix() {
    return discoverPrefix;
  }

  /**
   * Sets the discover prefix string that is used to establish a STAN session.
   * 
   * @param discoverPrefix the discoverPrefix to set
   */
  public void setDiscoverPrefix(String discoverPrefix) {
    this.discoverPrefix = discoverPrefix;
  }

  /**
   * Returns the maximum number of publish ACKs that may be in flight at any point in time.
   * 
   * @return the maxPubAcksInFlight
   */
  public int getMaxPubAcksInFlight() {
    return maxPubAcksInFlight;
  }

  /**
   * Sets the maximum number of publish ACKs that may be in flight at any point in time.
   * 
   * @param maxPubAcksInFlight the maxPubAcksInFlight to set
   */
  public void setMaxPubAcksInFlight(int maxPubAcksInFlight) {
    this.maxPubAcksInFlight = maxPubAcksInFlight;
  }

  /**
   * Returns the NATS connection URL.
   * 
   * @return the NATS connection URL
   */
  public String getNatsUrl() {
    return natsURL;
  }

  /**
   * Sets the NATS URL.
   * 
   * @param natsUrl the natsURL to set
   */
  public void setNatsUrl(String natsUrl) {
    this.natsURL = natsUrl;
  }

  /**
   * Returns the client ID of the current STAN session.
   * 
   * @return the client ID of the current STAN session
   */
  public String getClientId() {
    return clientID;
  }

  /**
   * Sets the client ID for the current STAN session.
   * 
   * @param clientId the clientID to set
   */
  public void setClientId(String clientId) {
    this.clientID = clientId;
  }

  /**
   * Returns the cluster ID of the current STAN session.
   * 
   * @return the clusterID
   */
  public String getClusterId() {
    return clusterID;
  }

  /**
   * Sets the cluster ID of the current STAN session.
   * 
   * @param clusterId the clusterID to set
   */
  public void setClusterId(String clusterId) {
    this.clusterID = clusterId;
  }
}

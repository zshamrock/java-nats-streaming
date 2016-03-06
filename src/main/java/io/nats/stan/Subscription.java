/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A client uses a {@code Subscription} object to receive messages that have been published to a
 * subject.
 * 
 * <p>
 * Each {@code Subscription} object is unique, even if the subscription is to the same subject. This
 * means that if {@code Connection.subscribe("foo", cb)} is called twice in a row, each of the
 * resulting {@code Subscription} objects will be unique, and any message delivered on subject "foo"
 * will be delivered individually to both {@code Subscription} objects.
 *
 */
public interface Subscription extends AutoCloseable {
  /**
   * Removes interest in the {@code Subscription} object's subject immediately.
   * 
   * @throws IOException if an error occurs while notifying the server
   * @throws TimeoutException if the unsubscribe request times out without a response
   */
  void unsubscribe() throws IOException, TimeoutException;

  /**
   * Removes interest in the {@code Subscription}'s subject immediately.
   * 
   * @see io.nats.client.Subscription#close()
   * @see java.lang.AutoCloseable#close()
   */
  void close();

  SubscriptionOptions getOptions();
}


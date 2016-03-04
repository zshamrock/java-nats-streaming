/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 
 *
 */
public interface Subscription extends AutoCloseable {

	void unsubscribe() throws IOException, TimeoutException;
	void close();
	SubscriptionOptions getOptions();
}

/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.stan;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public interface Connection extends AutoCloseable {
	/**
	 * @param subject
	 * @param data
	 * @throws IOException 
	 */
	void publish(String subject, byte[] data) throws IOException;
	String publish(String subject, byte[] data, AckHandler ah) throws IOException;
	void publish(String subject, String reply, byte[] data) throws IOException;
	String publish(String subject, String reply, byte[] data, AckHandler ah) throws IOException;
	Subscription subscribe(String subject, MessageHandler cb) throws IOException, TimeoutException;
	Subscription subscribe(String subject, MessageHandler cb, SubscriptionOptions opts) throws IOException, TimeoutException;
	Subscription subscribe(String subject, String queue, MessageHandler cb) throws IOException, TimeoutException;
	Subscription subscribe(String subject, String queue, MessageHandler cb, SubscriptionOptions opts) throws IOException, TimeoutException;
	void close() throws IOException, TimeoutException;
}

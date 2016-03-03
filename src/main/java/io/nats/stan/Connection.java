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

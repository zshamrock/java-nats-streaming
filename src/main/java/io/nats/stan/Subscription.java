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

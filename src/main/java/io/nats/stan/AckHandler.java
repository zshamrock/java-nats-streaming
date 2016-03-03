/**
 * 
 */
package io.nats.stan;

/**
 *
 *
 */
public interface AckHandler {
	public void onAck(String ack, Exception ex);
}

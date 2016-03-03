package io.nats.stan;

public interface MessageHandler {
	void onMessage(io.nats.stan.Message msg);
}

/**
 * 
 */
package io.nats.stan;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Duration;

/**
 * @author larry
 *
 */
public class SubscriptionOptions {
	// DurableName, if set will survive client restarts.
	String 			durableName;
	// Controls the number of messages the cluster will have inflight without an ACK.
	int				maxInFlight		= SubscriptionImpl.DEFAULT_MAX_IN_FLIGHT;
	// Controls the time the cluster will wait for an ACK for a given message.
	long			ackWait			= SubscriptionImpl.DEFAULT_ACK_WAIT;
	// StartPosition enum from proto.
	StartPosition 	startAt;
	// Optional start sequence number.
	long			startSequence;
	// Optional start time in nanoseconds since the UNIX epoch.
	long			startTime;
	// Option to do Manual Acks
	boolean			manualAcks;
	
	Date 			startTimeAsDate;
	
	private SubscriptionOptions(Builder builder) {
		this.durableName = builder.durableName;
		this.maxInFlight = builder.maxInFlight;
		this.ackWait = builder.ackWait;
		this.startAt = builder.startAt;
		this.startSequence = builder.startSequence;
		this.startTime = builder.startTime;
		this.manualAcks = builder.manualAcks;
	}
	
	public String getDurableName() {
		return durableName;
	}

	public int getMaxInFlight() {
		return maxInFlight;
	}

	public long getAckWait() {
		return ackWait;
	}
	public StartPosition getStartAt() {
		return startAt;
	}
	public long getStartSequence() {
		return startSequence;
	}
	public Date getStartTime() {
		if (startTimeAsDate == null)
			startTimeAsDate = new Date(getStartTime(TimeUnit.MILLISECONDS));
		return startTimeAsDate;
	}
	
	public long getStartTime(TimeUnit unit) {
		return unit.convert(startTime, TimeUnit.NANOSECONDS);
	}
	
	public boolean isManualAcks() {
		return manualAcks;
	}
	
	
	final public static class Builder {
		String 			durableName;
		int				maxInFlight		= SubscriptionImpl.DEFAULT_MAX_IN_FLIGHT;
		long			ackWait			= SubscriptionImpl.DEFAULT_ACK_WAIT;
		StartPosition 	startAt;
		long			startSequence;
		long			startTime;
		boolean			manualAcks;
		Date 			startTimeAsDate;

		public Builder setDurableName(String durableName) {
			this.durableName = durableName;
			return this;
		}
		
		public Builder setMaxInFlight(int maxInFlight) {
			this.maxInFlight = maxInFlight;
			return this;
		}
		
		public Builder setAckWait(long ackWait) {
			this.ackWait = ackWait;
			return this;
		}
		
		public Builder setAckWait(long time, TimeUnit unit) {
			this.ackWait = unit.toMillis(time);
			return this;
		}
		
		public Builder setManualAcks(boolean manualAcks) {
			this.manualAcks = manualAcks;
			return this;
		}

		public Builder startAtSequence(long seq) {
			this.startAt = StartPosition.SequenceStart;
			this.startSequence = seq;
			return this;
		}
		
		public Builder startAtTime(Date start) {
			this.startAt = StartPosition.TimeDeltaStart;
			this.startTime = TimeUnit.MILLISECONDS.toNanos(start.getTime());
			return this;
		}

		public Builder startAtTimeDelta(long ago, TimeUnit unit) {
			this.startAt = StartPosition.TimeDeltaStart;
			this.startTime = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis() - unit.toMillis(ago));
			return this;
		}
		
		public Builder startWithLastReceived() {
			this.startAt = StartPosition.LastReceived;
			return this;
		}
		
		public Builder deliverAllAvailable() {
			this.startAt = StartPosition.First;
			return this;
		}
		
		public SubscriptionOptions build() {
			return new SubscriptionOptions(this);
		}
	}
}

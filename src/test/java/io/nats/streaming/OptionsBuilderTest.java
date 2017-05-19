/*
 *  Copyright (c) 2017 Logimethods Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static io.nats.streaming.UnitTestUtilities.setupMockNatsConnection;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.nats.client.Connection;

@Category(UnitTest.class)
public class OptionsBuilderTest {

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    /**
     * Test method for {@link java.io.Serializable}.
     * @throws IOException 
     * @throws ClassNotFoundException 
     * @throws InterruptedException 
     */
    @Test
    public void testSerializable() throws ClassNotFoundException, IOException, InterruptedException {
    	Connection nc = setupMockNatsConnection();
    	Options.Builder testOptsBuilder = new Options.Builder()
    			.pubAckWait(Duration.ofMillis(500))
    			.connectWait(Duration.ofMillis(1500))
    			.discoverPrefix("PrEfiX")
    			.maxPubAcksInFlight(10000)
    			.natsConn(nc)
    			.natsUrl("nats://nats");
    	final Options.Builder serializedTestOpts = (Options.Builder) UnitTestUtilities.serializeDeserialize(testOptsBuilder);
    	assertTrue(equals(testOptsBuilder, serializedTestOpts));
    }
    
	protected static boolean equals(Options.Builder build1, Options.Builder build2) {
		if (build1 == build2)
			return true;
		Options obj1 = build1.build();
		Options obj2 = build2.build();
		
		if (obj1.getAckTimeout() == null) {
			if (obj2.getAckTimeout() != null)
				return false;
		} else if (!obj1.getAckTimeout().equals(obj2.getAckTimeout()))
			return false;
		if (obj1.connectTimeout == null) {
			if (obj2.connectTimeout != null)
				return false;
		} else if (!obj1.connectTimeout.equals(obj2.connectTimeout))
			return false;
		if (obj1.getDiscoverPrefix() == null) {
			if (obj2.getDiscoverPrefix() != null)
				return false;
		} else if (!obj1.getDiscoverPrefix().equals(obj2.getDiscoverPrefix()))
			return false;
		if (obj1.getMaxPubAcksInFlight() != obj2.getMaxPubAcksInFlight())
			return false;
		// natsConn is transient
/*		if (obj1.getNatsConn() == null) {
			if (obj2.getNatsConn() != null)
				return false;
		} else if (!obj1.getNatsConn().equals(obj2.getNatsConn()))
			return false;*/
		if (obj1.getNatsUrl() == null) {
			if (obj2.getNatsUrl() != null)
				return false;
		} else if (!obj1.getNatsUrl().equals(obj2.getNatsUrl()))
			return false;
		return true;
	}
}

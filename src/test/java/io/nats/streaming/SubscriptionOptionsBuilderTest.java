/*
 *  Copyright (c) 2017 Logimethods Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.streaming;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class SubscriptionOptionsBuilderTest {

    private static SubscriptionOptions.Builder testOptsBuilder;

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    /**
     * Setup for all cases in this test.
     * 
     * @throws Exception if something goes wrong
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        testOptsBuilder = new SubscriptionOptions.Builder().ackWait(Duration.ofMillis(500))
                .durableName("foo").manualAcks().maxInFlight(10000)
                .startAtSequence(12345);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    /**
     * Test method for {@link java.io.Serializable}.
     * @throws IOException 
     * @throws ClassNotFoundException 
     */
    @Test
    public void testSerializable() throws ClassNotFoundException, IOException {
        final SubscriptionOptions.Builder serializedTestOpts = (SubscriptionOptions.Builder) serializeDeserialize(testOptsBuilder);
        
        assertTrue(equals(testOptsBuilder, serializedTestOpts));
    }

	protected static Object serializeDeserialize(Object object)
			throws IOException, ClassNotFoundException {
		byte[] bytes = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		bos = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(bos);
		oos.writeObject(object);
		oos.flush();
		bytes = bos.toByteArray();
		oos.close();
		bos.close();

		Object obj = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		bis = new ByteArrayInputStream(bytes);
		ois = new ObjectInputStream(bis);
		obj = ois.readObject();
		bis.close();
		ois.close();
		return obj;
	}
	
	protected static boolean equals(SubscriptionOptions.Builder obj1, SubscriptionOptions.Builder obj2) {
		if (obj1 == obj2)
			return true;
		if (obj1.ackWait == null) {
			if (obj2.ackWait != null)
				return false;
		} else if (!obj1.ackWait.equals(obj2.ackWait))
			return false;
		if (obj1.durableName == null) {
			if (obj2.durableName != null)
				return false;
		} else if (!obj1.durableName.equals(obj2.durableName))
			return false;
		if (obj1.manualAcks != obj2.manualAcks)
			return false;
		if (obj1.maxInFlight != obj2.maxInFlight)
			return false;
		if (obj1.startAt != obj2.startAt)
			return false;
		if (obj1.startSequence != obj2.startSequence)
			return false;
		if (obj1.startTime == null) {
			if (obj2.startTime != null)
				return false;
		} else if (!obj1.startTime.equals(obj2.startTime))
			return false;
		if (obj1.startTimeAsDate == null) {
			if (obj2.startTimeAsDate != null)
				return false;
		} else if (!obj1.startTimeAsDate.equals(obj2.startTimeAsDate))
			return false;
		return true;
	}
    
}

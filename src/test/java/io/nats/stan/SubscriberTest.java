package io.nats.stan;

import static io.nats.stan.UnitTestUtilities.runServer;
import static io.nats.stan.UnitTestUtilities.testClusterName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.nats.stan.examples.Publisher;
import io.nats.stan.examples.Subscriber;

import ch.qos.logback.classic.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Category(IntegrationTest.class)
public class SubscriberTest {
    static final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger logger = (Logger) LoggerFactory.getLogger(SubscriberTest.class);

    static final LogVerifier verifier = new LogVerifier();

    static final String clusterId = UnitTestUtilities.testClusterName;

    ExecutorService service = Executors.newCachedThreadPool();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public TestCasePrinterRule pr = new TestCasePrinterRule(System.out);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @AfterClass
    public static void tearDownAfterClass() throws Exception {}

    @Before
    public void setUp() throws Exception {}

    @After
    public void tearDown() throws Exception {}

    @Test
    public void testSubscriberStringArray() throws Exception {
        String clusterId = UnitTestUtilities.testClusterName;

        List<String> argList = new ArrayList<String>();
        argList.addAll(Arrays.asList("-c", clusterId));
        argList.add("foo");

        String[] args = new String[argList.size()];
        args = argList.toArray(args);

        new Subscriber(args);
    }

    @Test
    public void testParseArgsBadFlags() {
        List<String> argList = new ArrayList<String>();
        String[] flags = new String[] { "-s", "-c", "-id", "-q", "--seq", "--since", "--durable" };
        boolean exThrown = false;

        for (String flag : flags) {
            try {
                exThrown = false;
                argList.clear();
                argList.addAll(Arrays.asList(flag, "foo"));
                String[] args = new String[argList.size()];
                args = argList.toArray(args);
                new Subscriber(args);
            } catch (IllegalArgumentException e) {
                assertEquals(String.format("%s requires an argument", flag), e.getMessage());
                exThrown = true;
            } finally {
                assertTrue("Should have thrown exception", exThrown);
            }
        }
    }

    @Test
    public void testParseArgsNotEnoughArgs() {
        List<String> argList = new ArrayList<String>();
        boolean exThrown = false;

        try {
            exThrown = false;
            argList.clear();
            String[] args = new String[argList.size()];
            args = argList.toArray(args);
            new Subscriber(args);
        } catch (IllegalArgumentException e) {
            assertEquals("must supply at least a subject name", e.getMessage());
            exThrown = true;
        } finally {
            assertTrue("Should have thrown exception", exThrown);
        }
    }

    @Test(timeout = 5000)
    public void testMainSuccess() throws Exception {
        try (StanServer srv = runServer(testClusterName)) {
            Publisher.main(new String[] { "-c", clusterId, "foo", "bar" });
            Subscriber.main(new String[] { "-c", clusterId, "--all", "--count", "1", "foo" });
        }
    }

    @Test
    public void testMainFailsNoServers() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage(io.nats.client.Constants.ERR_NO_SERVERS);
        Subscriber.main(new String[] { "-s", "nats://enterprise:4242", "foobar" });
    }

    @Test
    public void testMainFailsTimeout() throws Exception {
        thrown.expect(TimeoutException.class);
        thrown.expectMessage(ConnectionImpl.ERR_CONNECTION_REQ_TIMEOUT);
        Subscriber.main(new String[] { "foobar" });
    }
}

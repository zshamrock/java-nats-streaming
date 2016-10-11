/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.stan.examples;

import io.nats.benchmark.Benchmark;
import io.nats.benchmark.Sample;
import io.nats.client.AsyncSubscription;
import io.nats.client.ClosedCallback;
import io.nats.client.ConnectionEvent;
import io.nats.client.DisconnectedCallback;
import io.nats.client.ExceptionHandler;
import io.nats.client.NATSException;
import io.nats.stan.AckHandler;
import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;
import io.nats.stan.SubscriptionOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility class for measuring NATS performance.
 *
 */
public class StanBench {
    static final Logger log = LoggerFactory.getLogger(StanBench.class);

    // Default test values
    private int numMsgs = 100000;
    private int numPubs = 1;
    private int numSubs = 0;
    private boolean async = false;
    private int size = 128;
    boolean ignoreOld = false;
    int maxPubAcksInFlight = 1000;
    String clientId = "benchmark";
    String clusterId = "test-cluster";

    private String urls = io.nats.client.ConnectionFactory.DEFAULT_URL;
    private String subject;
    private AtomicInteger published = new AtomicInteger();
    private AtomicInteger received = new AtomicInteger();
    private String csvFileName;

    private io.nats.client.ConnectionFactory natsConnFactory;
    private Thread shutdownHook;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private boolean secure;
    private Benchmark bench;

    static final String usageString =
            "\nUsage: nats-bench [-s server] [--tls] [-np num] [-ns num] [-n num] [-ms size] "
                    + "[-csv file] <subject>\n\nOptions:\n"
                    + "    -s   <urls>                     NATS server URLs (separated by comma)\n"
                    + "    -tls                            Use TLS secure connection\n"
                    + "    -np                             Number of concurrent publishers\n"
                    + "    -ns                             Number of concurrent subscribers\n"
                    + "    -n                              Number of messages to publish\n"
                    + "    -a                              Async message publishing\n"
                    + "    -ms                             Message size in bytes\n"
                    + "    -io                             Subscribers ignore old messages\n"
                    + "    -ms                             Message size in bytes\n"
                    + "    -mpa                            Max number of published acks in flight\n"
                    + "    -id                             Benchmark process base client ID\n"
                    + "    -csv                            Save bench data to csv file\n";

    /**
     * Main constructor for StanBench.
     * 
     * @param args configuration parameters
     */
    public StanBench(String[] args) {
        if (args == null || args.length < 1) {
            usage();
            return;
        }
        parseArgs(args);
        natsConnFactory = new io.nats.client.ConnectionFactory(urls);
        natsConnFactory.setSecure(secure);
        natsConnFactory.setReconnectAllowed(false);

        bench = new Benchmark("NATS Streaming", numSubs, numPubs);
    }

    class Worker implements Runnable {
        protected final Phaser phaser;
        protected final int num;
        protected final int size;
        protected final boolean ignoreOld;
        protected final String workerClientId;

        Worker(Phaser phaser, int numMsgs, int size, boolean ignoreOld, String workerClientId) {
            this.phaser = phaser;
            this.num = numMsgs;
            this.size = size;
            this.ignoreOld = ignoreOld;
            this.workerClientId = workerClientId;
        }

        @Override
        public void run() {}
    }

    class SubWorker extends Worker {
        SubWorker(Phaser phaser, int numMsgs, int size, boolean ignoreOld, String subId) {
            super(phaser, numMsgs, size, ignoreOld, subId);
        }

        @Override
        public void run() {
            try {
                runSubscriber();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runSubscriber() throws Exception {
            final ConnectionFactory cf = new ConnectionFactory(clusterId, workerClientId);

            final io.nats.client.Connection nc = natsConnFactory.createConnection();
            nc.setDisconnectedCallback(new DisconnectedCallback() {
                @Override
                public void onDisconnect(ConnectionEvent ev) {
                    log.error("Subscriber disconnected after {} msgs", received.get());
                }
            });
            nc.setClosedCallback(new ClosedCallback() {
                @Override
                public void onClose(ConnectionEvent ev) {
                    log.error("Subscriber connection closed after {} msgs", received.get());
                }
            });
            nc.setExceptionHandler(new ExceptionHandler() {
                @Override
                public void onException(NATSException ex) {
                    log.error("Subscriber connection exception", ex);
                    AsyncSubscription sub = (AsyncSubscription) ex.getSubscription();
                    log.error("Sent={}, Received={}", published.get(), received.get());
                    log.error("Messages dropped (total) = {}", sub.getDropped());
                    System.exit(-1);
                }
            });
            cf.setNatsConnection(nc);

            final Connection sc = cf.createConnection();
            final SubscriptionOptions sopts;

            if (ignoreOld) {
                sopts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
            } else {
                sopts = new SubscriptionOptions.Builder().build();
            }

            final long start = System.nanoTime();

            final Subscription sub = sc.subscribe(subject, new MessageHandler() {
                @Override
                public void onMessage(Message msg) {
                    received.incrementAndGet();
                    if (received.get() >= numMsgs) {
                        bench.addSubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
                        phaser.arrive();
                        nc.setDisconnectedCallback(null);
                        nc.setClosedCallback(null);
                        try {
                            sc.close();
                        } catch (IOException | TimeoutException e) {
                            log.warn(
                                    "stan-bench: "
                                            + "exception thrown during subscriber connection close",
                                    e);
                        }
                    }
                }
            }, sopts);

            phaser.arrive();
            while (received.get() < numMsgs) {
            }
        }
    }


    class PubWorker extends Worker {
        PubWorker(Phaser phaser, int numMsgs, int size, boolean ignoreOld, String pubId) {
            super(phaser, numMsgs, size, ignoreOld, pubId);
        }

        @Override
        public void run() {
            try {
                runPublisher();
                phaser.arrive();
            } catch (Exception e) {
                e.printStackTrace();
                phaser.arrive();
            }
        }

        public void runPublisher() throws Exception {
            final ConnectionFactory cf = new ConnectionFactory(clusterId, workerClientId);
            if (maxPubAcksInFlight > 0) {
                cf.setMaxPubAcksInFlight(maxPubAcksInFlight);
            }

            final io.nats.client.Connection nc = natsConnFactory.createConnection();
            cf.setNatsConnection(nc);

            try (Connection sc = cf.createConnection()) {
                byte[] msg = null;
                if (size > 0) {
                    msg = new byte[size];
                }

                final long start = System.nanoTime();

                if (async) {
                    CountDownLatch latch = new CountDownLatch(1);
                    AckHandler acb = new AckHandler() {
                        public void onAck(String nuid, Exception ex) {
                            if (published.incrementAndGet() >= numMsgs) {
                                latch.countDown();
                            }
                        }
                    };
                    for (int i = 0; i < numMsgs; i++) {
                        try {
                            sc.publish(subject, msg, acb);
                        } catch (Exception e) {
                            log.error("stan-bench: error during publish", e);
                        }
                    }
                    latch.await();
                } else {
                    for (int i = 0; i < numMsgs; i++) {
                        try {
                            sc.publish(subject, msg);
                            published.incrementAndGet();
                        } catch (Exception e) {
                            log.error("stan-bench: error during publish", e);
                        }
                    }
                }

                bench.addPubSample(new Sample(numMsgs, size, start, System.nanoTime(), nc));
            } // Connection
        }
    }

    /**
     * Runs the benchmark.
     * 
     * @throws Exception if an exception occurs
     */
    public void run() throws Exception {
        ExecutorService exec = Executors.newCachedThreadPool();
        final Phaser phaser = new Phaser();

        installShutdownHook();

        phaser.register();

        // Run Subscribers first
        for (int i = 0; i < numSubs; i++) {
            phaser.register();
            String subId = String.format("%s-sub-%d", clientId, i);
            exec.execute(new SubWorker(phaser, numMsgs, size, ignoreOld, subId));
        }

        // Wait for subscribers threads to initialize
        phaser.arriveAndAwaitAdvance();

        // Now publishers
        for (int i = 0; i < numPubs; i++) {
            phaser.register();
            String subId = String.format("%s-pub-%d", clientId, i);
            exec.execute(new PubWorker(phaser, numMsgs, size, ignoreOld, subId));
        }

        System.out.printf("Starting benchmark [msgs=%d, msgsize=%d, pubs=%d, subs=%d]\n", numMsgs,
                size, numPubs, numSubs);

        // Wait for subscribers and publishers to finish
        phaser.arriveAndAwaitAdvance();

        // We're done. Clean up and report.
        Runtime.getRuntime().removeShutdownHook(shutdownHook);

        bench.close();
        System.out.println(bench.report());

        if (csvFileName != null) {
            List<String> csv = bench.csv();
            Path csvFile = Paths.get(csvFileName);
            Files.write(csvFile, csv, Charset.forName("UTF-8"));
        }
    }

    void installShutdownHook() {
        shutdownHook = new Thread(new Runnable() {
            @Override
            public void run() {
                System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                shutdown.set(true);
                System.err.printf("Sent=%d\n", published.get());
                System.err.printf("Received=%d\n", received.get());

            }
        });

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    long backlog() {
        return published.get() - received.get();
    }

    private void parseArgs(String[] args) {
        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        subject = argList.get(argList.size() - 1);
        argList.remove(argList.size() - 1);


        // Anything left is flags + args
        Iterator<String> it = argList.iterator();
        while (it.hasNext()) {
            String arg = it.next();
            switch (arg) {
                case "-s":
                case "--server":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    urls = it.next();
                    it.remove();
                    continue;
                case "--tls":
                    secure = true;
                    it.remove();
                    continue;
                case "-np":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numPubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-ns":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numSubs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-n":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    numMsgs = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-a":
                    async = true;
                    it.remove();
                    continue;
                case "-ms":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    size = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-io":
                    ignoreOld = true;
                    it.remove();
                    continue;
                case "-mpa":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    maxPubAcksInFlight = Integer.parseInt(it.next());
                    it.remove();
                    continue;
                case "-id":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-csv":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    csvFileName = it.next();
                    it.remove();
                    continue;

                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
    }

    /**
     * The main program executive.
     * 
     * @param args command line arguments
     */
    public static void main(String[] args) {
        try {
            new StanBench(args).run();
        } catch (Exception e) {

            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }

}

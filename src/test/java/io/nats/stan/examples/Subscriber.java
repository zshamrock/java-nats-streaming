package io.nats.stan.examples;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;
import io.nats.stan.SubscriptionOptions;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Subscriber {
    private String url;
    private String subject;
    private String clusterId = "test-cluster";
    private String clientId = "test-client";
    private SubscriptionOptions.Builder builder = new SubscriptionOptions.Builder();
    private String qgroup;
    private String durable;
    private long seq = -1L;
    private boolean all;
    private boolean last;
    private Duration since;

    static final String usageString = "\nUsage: java Subscriber [options] <subject>\n\nOptions:\n"
            + "    -s,  --server   <url>            STAN server URL(s)\n"
            + "    -c,  --cluster  <cluster name>   STAN cluster name\n"
            + "    -id, --clientid <client ID>      STAN client ID               \n\n"
            + "Subscription Options:                                             \n"
            + "     -q, --qgroup <name>             Queue group\n"
            + "         --seq    <seqno>            Start at seqno\n"
            + "         --all                       Deliver all available messages\n"
            + "         --last                      Deliver starting with last published message\n"
            + "         --since  <duration>         Deliver messages in last interval "
            + "(e.g. 1s, 1hr)\n" + "                   (format: 00d00h00m00s00ns)\n"
            + "         --durable <name>            Durable subscriber name";

    Subscriber(String[] args) {
        parseArgs(args);
        if (subject == null) {
            usage();
        }
    }

    void usage() {
        System.err.println(usageString);
        System.exit(-1);
    }

    void run() throws IOException, TimeoutException {
        ConnectionFactory cf = new ConnectionFactory(clusterId, clientId);
        if (url != null) {
            cf.setNatsUrl(url);
        }

        try (final Connection sc = cf.createConnection()) {
            // System.out.println("Connected successfully to " + cf.getNatsUrl());
            AtomicInteger count = new AtomicInteger();
            try (final Subscription sub = sc.subscribe(subject, qgroup, new MessageHandler() {
                public void onMessage(Message msg) {
                    System.out.printf("[#%d] Received on [%s]: '%s'\n", count.incrementAndGet(),
                            msg.getSubject(), msg);
                }
            }, builder.build())) {
                System.err.printf("Listening on [%s], clientID=[%s], qgroup=[%s] durable=[%s]\n",
                        sub.getSubject(), clientId, sub.getQueue(),
                        sub.getOptions().getDurableName());
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    public void run() {
                        try {
                            System.err.println("\nCaught CTRL-C, shutting down gracefully...\n");
                            sub.unsubscribe();
                            sc.close();
                        } catch (IOException | TimeoutException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                });
                while (true) {
                    // loop forever
                }
            }
        }
    }

    private void parseArgs(String[] args) {
        if (args == null || args.length < 1) {
            usage();
            return;
        }

        List<String> argList = new ArrayList<String>(Arrays.asList(args));

        // The last arg should be subject
        // get the subject and remove it from args
        subject = argList.remove(argList.size() - 1);

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
                    url = it.next();
                    it.remove();
                    continue;
                case "-c":
                case "--cluster":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clusterId = it.next();
                    it.remove();
                    continue;
                case "-id":
                case "--clientid":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    clientId = it.next();
                    it.remove();
                    continue;
                case "-q":
                case "--qgroup":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    qgroup = it.next();
                    it.remove();
                    continue;
                case "--seq":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    builder.startAtSequence(Long.parseLong(it.next()));
                    it.remove();
                    continue;
                case "--all":
                    builder.deliverAllAvailable();
                    it.remove();
                    continue;
                case "--last":
                    builder.startWithLastReceived();
                    it.remove();
                    continue;
                case "--since":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    try {
                        builder.startAtTimeDelta(parseDuration(it.next()));
                    } catch (ParseException e) {
                        e.printStackTrace();
                        usage();
                    }
                    it.remove();
                    continue;
                case "--durable":
                    if (!it.hasNext()) {
                        usage();
                    }
                    it.remove();
                    builder.setDurableName(it.next());
                    it.remove();
                    continue;
                default:
                    System.err.printf("Unexpected token: '%s'\n", arg);
                    usage();
                    break;
            }
        }
        if (qgroup != null && durable != null) {
            System.err.println("Durable subscription cannot be used with queue group");
            usage();
        }

    }

    private static Pattern p =
            Pattern.compile("(\\d+)d\\s*(\\d+)h\\s*(\\d+)m\\s*(\\d+)s\\s*(\\d+)ns");

    /**
     * Parses a duration string of the form "98d 01h 23m 45s" into milliseconds.
     * 
     * @throws ParseException if the duration can't be parsed
     */
    public static Duration parseDuration(String duration) throws ParseException {
        Matcher m = p.matcher(duration);

        long nanoseconds = 0L;

        if (m.find() && m.groupCount() == 4) {
            int days = Integer.parseInt(m.group(1));
            nanoseconds += TimeUnit.NANOSECONDS.convert(days, TimeUnit.DAYS);
            int hours = Integer.parseInt(m.group(2));
            nanoseconds += TimeUnit.NANOSECONDS.convert(hours, TimeUnit.HOURS);
            int minutes = Integer.parseInt(m.group(3));
            nanoseconds += TimeUnit.NANOSECONDS.convert(minutes, TimeUnit.MINUTES);
            int seconds = Integer.parseInt(m.group(4));
            nanoseconds += TimeUnit.NANOSECONDS.convert(seconds, TimeUnit.SECONDS);
            long nanos = Long.parseLong(m.group(5));
            nanoseconds += nanos;
        } else {
            throw new ParseException("Cannot parse duration " + duration, 0);
        }

        return Duration.ofNanos(nanoseconds);
    }

    /**
     * Subscribes to a subject.
     * 
     * @param args the subject, cluster info, and subscription options
     */
    public static void main(String[] args) {
        try {
            new Subscriber(args).run();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.exit(0);
    }
}

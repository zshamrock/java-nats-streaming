Change Log
==========

## Version 0.3.0
_2016-09-14_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.2.1...0.3.0)
 * [#29] Added `getNatsConnection()` public API for getting underlying NATS connection.
 * Removed the `publish*WithReply` variants, as request-reply isn't well-served by streaming. Use the underlying NATS connection to do this.

## Version 0.2.1
_2016-09-12_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.2.0...0.2.1)
 * [#26](/../../issues/#26) Fixed an issue where the heartbeat subscription was not being unsubscribed during connection close.
 * Updated README examples and installation info

## Version 0.2.0
_2016-09-10_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.1.2...0.2.0)

 * Fix `ConnectionFactory::createConnection` to return `Connection` interface vs. `ConnectionImpl`
 * Resolved several test issues, including locating/running server binary for integration tests

## Version 0.1.2
_2016-08-29_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/0.1.1...0.1.2)

 * Filter nats_checkstyle.xml from jar/bundle 

## Version 0.1.1
_2016-08-26_    [GitHub Diff](https://github.com/nats-io/java-nats-streaming/compare/v0.1.0...0.1.1)

 * Updated dependencies and build configuration

## Version 0.1.0
_2016-08-26_

_Initial public release of java-nats-streaming, now available on Maven Central._



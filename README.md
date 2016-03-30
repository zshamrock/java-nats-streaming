![](https://raw.githubusercontent.com/nats-io/nats-site/master/src/img/large-logo.png)
# Project STAN Java Client
STAN is an extremely performant, lightweight reliable streaming platform powered by NATS.

[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)
[![Javadoc](http://javadoc-badge.appspot.com/io.nats/stan-java-client.svg?label=javadoc)](http://nats-io.github.io/stan-java-client)
[![Build Status](https://travis-ci.com/nats-io/stan-java-client.svg?branch=master)](http://travis-ci.com/nats-io/stan-java-client)

## Installation

```
git clone git@github.com:/nats-io/stan-java-client.git

mvn install
```

Load the following dependency in your project's pom.xml:

```
  <dependencies>
    ...
    <dependency>
      <groupId>io.nats</groupId>
      <artifactId>jstan</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

## Documentation

Once installation is complete, the javadoc can be accessed via `target/apidocs/index.html`

## Known Issues and Notes for the STAN Preview

- Download a server binary from the [STAN Server Preview releases page](https://github.com/nats-io/stan-server-preview/releases)
- See the [STAN Go client README](https://github.com/nats-io/stan) for more details

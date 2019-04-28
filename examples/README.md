This directory contains example programs for the various utility classes.

**BEWARE:** these programs will incur AWS charges when run. You are responsible for those charges.


## Building and Running

Use Maven to build the project. If you're working on a snapshot version, you will also need to build
the parent project first.

    mvn clean package

The result is an "uberjar," which contains all dependencies needed to run. This means that it can
serve as the Java classpath:

    java -cp target/aws-java-utils-examples-*.jar FULLY_QUALIFIED_CLASSNAME [ ARGUMENTS ]


## The Programs

[`MetricReporterExample`](src/main/java/com/kdgregory/aws/utils/examples/cloudwatch/MetricReporterExample.java)
reports the results of a "random walk" as a high-resolution CloudWatch metric. This is intended to simulate an
IoT sensor, although _CloudWatch should never be used to control industrial processes._

# aws-utils

A collection of utility classes that work with the AWS v1 Java SDK, and provide a simpler
way to accomplish some tasks.

These utilities have been developed to support my work with AWS, and target the services
that I use the most. I have packaged them to simplify use with future projects. Updates
will be infrequent; at this point in time I don't work extensively with Java. I _do not_
plan to release a version that supports the v2 SDK.

> Because this library represents a personal toolchest, I am not currently accepting
  pull requests. If you find a bug in the current functionality, however, please open
  an issue, and I will look at it as soon as possible.


## Usage

### Dependencies

Include this JAR in your project dependencies. For example, with Maven:

```
<dependency>
    <groupId>com.kdgregory.aws</groupId>
    <artifactId>aws-java-utils</artifactId>
    <version>${cka-utils.version}</version>
</dependency>
```

To find the current version, go to [search.maven.org](https://search.maven.org/classic/#search%7Cga%7C1%7Cg%3A%22com.kdgregory.aws%22%20AND%20a%3A%22aws-java-utils%22).

You must also include the AWS SDK JARs for all services that you use. The minimum supported
SDK version is 1.11.250.

The minimum supported Java version is 1.8.


### Package Structure

The top-level package is `com.kdgregory.aws.utils`. Under this are packages named after AWS
services (eg: `com.kdgregory.aws.utils.kinesis`).

Within each package there is a class containing static utility methods, named after the service
(example: `KinesisUtil`). There may also be instantiable classes (example: `KinesisReader`).


### AWS Service Clients

All operations and classes must be provided with an appropriate AWS client. Per AWS docs,
clients are intended to be long-lived, and shared between multiple threads.


### Logging

This library uses [commons-logging](http://commons.apache.org/proper/commons-logging/) version
1.1.3, which is also a dependency of the AWS SDK. Unlike the SDK, it does not have establish a
transitive dependency on commons-logging. If you want to use a logging framework that's not
supported by commons-logging, you'll need to explicitly exclude the latter from the SDK. The
[example POM](https://github.com/kdgregory/aws-java-utils/blob/dev-README/examples/pom.xml#L71)
shows how to do this for SLF4J/Logback.

> **Warning:** if you exclude the SDK's transitive dependency and do not provide a replacement,
  some operations will throw a `NoClassDefFoundError`.

> Note: the tests for the main library have a dependency on Log4J 1.x, because some tests
  look at logging output and I already had an "interceptor" for that framework. The examples
  and integration tests use Logback.

Log messages use either DEBUG or WARN level: the former to report progress, and the latter
for unexpected situations (such as a timeout expiring before a resource becomes ready). The
library does not use info- or error-level logging (I believe that errors should be reserved
for situations where someone needs to be woken up, and only you can decide that).

Loggers use the class name as logger name. With a hierarchical logging framework such as Log4J,
you can turn off all logging with the top-level package name:

```
log4j.logger.com.kdgregory.aws.utils=OFF
```


### Exceptions

Where appropriate, this library catches exceptions thrown by the SDK. For example, when
writing to CloudWatch Logs the sequence token may be invalidated by a concurrent writer.
In this case the library can transparently retry the operation.

However, many exceptions are non-recoverable, and are allowed to propagate. Your code
should always be prepared to handle exceptions thrown by the underlying APIs.


### Throttling and Retries

Any AWS operation is subject to throttling: if you invoke the operation too frequently,
it will throw an exception. Most of the utility methods are resilient to throttling,
and will retry their operation. Those that aren't are documented as such.


### Batching and Background Execution

Most operations are intended to be called synchronously: the calling thread will wait
until the operation completes. In some cases, particularly classes that write to the
service, it is more efficient to batch operations and invoke them asynchronously.

Classes that support background operation provide a constructor that takes  a Java
`ExecutorService` instance: my intention is that the caller will create a shared
executor that will service multiple objects. The number of threads managed by this
executor will depend on the number and frequency of background operations, but in
general you don't need many threads: each operation takes 50-100 ms.

For classes that support batched operations, the executor must be an instance of
`ScheduledExecutorService`, and the class will also be constructed with a scheduling
interval. The library class will perform some action (typically writing a batch of
messages) every time the interval expires. If there is nothing to do, the background
operation will still run but won't contact the service.

In either case, if you shut down the executor service you may prevent any outstanding
operations from completing. Library classes that support background operation provide
a `shutdown()` method, which will attempt to complete any pending operations and then
prevent future operations from being submitted.

This caveat also applies to the application as a whole: if you call `System.exit()`
while operations are queued, they will never complete. You can add a shutdown hook
to guard against this case, but you can't guard against unexpected JVM termination.
You will need to determine whether the trade-off between efficiency and certainty
is appropriate for your application.


# Building

## Directory structure

This project contains three directories:

* `library` is the main library of utility classes.
* `testhelpers` contains mock objects and other classes to support testing the library. This
  will be published in case it's useful for anyone else.
* `examples` contains programs that demonstrate the library.
* `integration-tests` are tests that exercise the library classes against actual AWS services.
  *Beware:* running these tests will incur service charges.


## Versioning

I follow the standard `MAJOR.MINOR.PATCH` versioning scheme:

* `MAJOR` tracks the AWS SDK major version (always 1)
* `MINOR` tracks the AWS SDK minor version (always 11).
* `PATCH` is incremented whenever functionality is added/fixed.

I will not make breaking API changes, unless such changes represent a complete misunderstanding on my
part about how the service operates. In that case, I will deprecate the relevant classes/methods for
one release, and update their JavaDoc to warn against their use.
  
Not all versions will be released to Maven Central. I may choose to make release (non-snapshot) versions for
development testing, or as interim steps of a bigger piece of functionality. However, all release versions
are tagged in source control, whether or not available on Maven Central.


## Source Control

The `trunk` branch is intended for "potentially releasable" versions. Commits on trunk are
functional, but may not be "complete" (for some definition of that word). They may be
"snapshot" or release builds. Trunk will never be rebased; once a commit is made there it's
part of history for better or worse.

Development takes place on a `dev-MAJOR.MINOR.PATCH` branch; these branches are deleted
once their content has been merged into `master`. *BEWARE*: these branches may be rebased
as I see fit.

Each "release" version is tagged with `release-MAJOR.MINOR.PATCH`, whether or not it was
uploaded to Maven Central.

# libHBase 

Native client for Apache HBase
This is a JNI based, thread safe C library that implements an HBase client.

## Building the native client
```
  mvn install -Phbase-native-client -DskipTests
```

This will build the tarball containing the headers, shared library and the jar
files in the `target` directory with the following structure.

```
/
+---bin/
+---conf/
+---include/
|   +--hbase/
+---lib/
|   +---native/
+---src
    +---examples/
    |   +---async/
    +---test/
        +---native/
            +---common/
```

The headers can be found under `include` folder while the shared library to link
against is under `lib/native`.

## Building and Running Unit Tests
libHBase uses [GTest](https://code.google.com/p/googletest/) as the test framework
for unit/integration tests. During the build process, it automatically downloads
and build the GTest. You will need to have `cmake` installed on the build machine
to build the GTest framwork.

Runnig the unit tests currently requires you to set `LIBHBASE_ZK_QUORUM` to a valid
HBase Zookeeper quorum. The default is `"localhost:2181"`. This can be either set
as an environment variable or in [this configuration file](src/test/resources/config.properties).
```
LIBHBASE_ZK_QUORUM="<zk_host>:<zk_port>,..." mvn integration-test
```

## Building Applications with libHBase
For examples on how to use the APIs, please take a look at [this sample source]
(src/examples/async/example_async.c).

As the library uses JNI, you will need to have both `libhbase` and `libjvm` shared
libraries in your application's library search path. The jars required for the
library can be specified through either of the environment variables `CLASSPATH`
or `HBASE_LIB_DIR`. Custom JVM options, for example `-Xmx`, etc can be specified
using the environment variable `LIBHBASE_OPTS`.

## Performance Testing
A performance test is included with the library which currently support sequential/
random gets and puts. You can run the tests using this [shell script](bin/perftest.sh).

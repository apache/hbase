Example code.

* org.apache.hadoop.hbase.mapreduce.SampleUploader
    Demonstrates uploading data from text files (presumably stored in HDFS) to HBase.

* org.apache.hadoop.hbase.mapreduce.IndexBuilder
    Demonstrates map/reduce with a table as the source and other tables as the sink.
    You can generate sample data for this MR job via hbase-examples/src/main/ruby/index-builder-setup.rb.


* Thrift examples
    Sample clients of the HBase ThriftServer. They perform the same actions, implemented in
    C++, Java, Ruby, PHP, Perl, and Python. Pre-generated Thrift code for HBase is included
    to be able to compile/run the examples without Thrift installed.
    If desired, the code can be re-generated as follows:
    thrift --gen cpp --gen java --gen rb --gen py --gen php --gen perl \
        ${HBASE_ROOT}/hbase-server/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift
    and re-placed at the corresponding paths.

    Before you run any Thrift examples, find a running HBase Thrift server.
    If you start one locally (bin/hbase thrift start), the default port is 9090.

    * Java: org.apache.hadoop.hbase.thrift.DemoClient (jar under lib/).
      1. Set up the classpath with all the necessary jars, for example:
        for f in `find . -name "libthrift-*.jar" -or -name "slf4j-*.jar" -or -name "log4j-*.jar"`; do
          HBASE_EXAMPLE_CLASSPATH=${HBASE_EXAMPLE_CLASSPATH}:$f;
        done
      2. If HBase server is not secure, or authentication is not enabled for the Thrift server, execute:
      {java -cp hbase-examples-[VERSION].jar:${HBASE_EXAMPLE_CLASSPATH} org.apache.hadoop.hbase.thrift.DemoClient <host> <port>}
      3. If HBase server is secure, and authentication is enabled for the Thrift server, run kinit at first, then execute:
      {java -cp hbase-examples-[VERSION].jar:${HBASE_EXAMPLE_CLASSPATH} org.apache.hadoop.hbase.thrift.DemoClient <host> <port> true}

    * Ruby: hbase-examples/src/main/ruby/DemoClient.rb
      1. Modify the import path in the file to point to {$THRIFT_HOME}/lib/rb/lib.
      2. Execute {ruby DemoClient.rb} (or {ruby DemoClient.rb <host> <port>}).

    * Python: hbase-examples/src/main/python/DemoClient.py
      1. Modify the added system path in the file to point to {$THRIFT_HOME}/lib/py/build/lib.[YOUR SYSTEM]
      2. Execute {python DemoClient.py <host> <port>}.

    * PHP: hbase-examples/src/main/php/DemoClient.php
      1. Modify the THRIFT_HOME path in the file to point to actual {$THRIFT_HOME}.
      2. Execute {php DemoClient.php}.
      3. Starting from Thrift 0.9.0, if Thrift.php complains about some files it cannot include, go to thrift root,
        and copy the contents of php/lib/Thrift under lib/php/src. Thrift.php appears to include, from under the same root,
        both TStringUtils.php, only present in src/, and other files only present under lib/; this will bring them under
        the same root (src/).
        If you know better about PHP and Thrift, please feel free to fix this.

    * Perl: hbase-examples/src/main/perl/DemoClient.pl
      1. Modify the "use lib" path in the file to point to {$THRIFT_HOME}/lib/perl/lib.
      2. Use CPAN to get Bit::Vector and Class::Accessor modules if not present (see thrift perl README if more modules are missing).
      3. Execute {perl DemoClient.pl}.

    * CPP: hbase-examples/src/main/cpp/DemoClient.cpp
      1. Make sure you have boost and Thrift C++ libraries; modify Makefile if necessary.
        The recent (0.9.0 as of this writing) version of Thrift can be downloaded from http://thrift.apache.org/download/.
        Boost can be found at http://www.boost.org/users/download/.
      2. Execute {make}.
      3. Execute {./DemoClient}.


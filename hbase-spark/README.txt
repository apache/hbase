
ON PROTOBUFS
This maven module has core protobuf definition files ('.protos') used by hbase
Spark.

Generation of java files from protobuf .proto files included here is done apart
from the build. Run the generation whenever you make changes to the .orotos files
and then check in the produced java (The reasoning is that change is infrequent
so why pay the price of generating files anew on each build.

To generate java files from protos run:

 $ mvn compile -Dcompile-protobuf
or
 $ mvn compile -Pcompile-protobuf

After you've done the above, check it and then check in changes (or post a patch
on a JIRA with your definition file changes and the generated files). Be careful
to notice new files and files removed and do appropriate git rm/adds.

Running Tests under hbase-spark module
Tests are run via ScalaTest Maven Plugin and Surefire Maven Plugin
The following are examples to run the tests:

Run tests under root dir or hbase-spark dir
 $ mvn test                  //run all small and medium java tests, and all scala tests
 $ mvn test -PskipSparkTests //skip all scale and java test in hbase-spark
 $ mvn test -P runAllTests   //run all tests, including scala and all java test including the large test

Run specified test case
  $ mvn test -Dtest=TestJavaHBaseContext -DwildcardSuites=None                        //java unit test
  $ mvn test -Dtest=None -DwildcardSuites=org.apache.hadoop.hbase.spark.BulkLoadSuite //scala unit test

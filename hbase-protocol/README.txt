These are the protobuf definition files used by hbase. The produced java
classes are generated into src/main/java/org/apache/hadoop/hbase/protobuf/generated
and then checked in.  The reasoning is that they change infrequently.

To regenerate the classes after making definition file changes, ensure first that
the protobuf protoc tool is in your $PATH (You may need to download it and build
it first; its part of the protobuf package obtainable from here: 
https://github.com/google/protobuf/releases/tag/v2.5.0).

HBase uses hadoop-maven-plugins:protoc goal to invoke the protoc command. You can 
compile the protoc definitions by invoking maven with profile compile-protobuf or 
passing in compile-protobuf property. 

mvn compile -Dcompile-protobuf
or
mvn compile -Pcompile-protobuf

You may also want to define protoc.path for the protoc binary

mvn compile -Dcompile-protobuf -Dprotoc.path=/opt/local/bin/protoc

If you have added a new proto file, you should add it to the pom.xml file first.
Other modules also support the maven profile.

After you've done the above, check it in and then check it in (or post a patch
on a JIRA with your definition file changes and the generated files).


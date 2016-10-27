These are the protobuf definition files used by hbase Coprocessor Endpoints.
HBase core uses protos found at hbase-protocol-shaded/src/main/protos. The
protos here are also in hbase-module-shaded though they are not exactly
the same files (they generate into different location; where to generate
to is part of the .proto file). Consider whether any changes made belong
both here and over in hbase-module-shaded.

The produced java classes are generated and then checked in. The reasoning
is that they change infrequently and it saves generating anew on each build.

To regenerate the classes after making definition file changes, ensure first that
the protobuf protoc tool is in your $PATH. You may need to download it and build
it first; its part of the protobuf package. For example, if using v2.5.0 of
protobuf, it is obtainable from here:

 https://github.com/google/protobuf/releases/tag/v2.5.0

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

NOTE: The protoc used here is probably NOT the same as the hbase-protocol-shaded
module uses; here we use a more palatable version -- 2.5.0 -- wherease over in
the internal hbase-protocol-shaded module, we'd use something newer. Be conscious
of this when running your protoc being sure to apply the appropriate version
per module.

After you've done the above, check it in and then check it in (or post a patch
on a JIRA with your definition file changes and the generated files).

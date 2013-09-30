These are the protobuf definition files used by hbase. The produced java
classes are generated into src/main/java/org/apache/hadoop/hbase/protobuf/generated
and then checked in.  The reasoning is that they change infrequently.

To regenerate the classes after making definition file changes, ensure first that
the protobuf protoc tool is in your $PATH (You may need to download it and build
it first; its part of the protobuf package obtainable from here: 
http://code.google.com/p/protobuf/downloads/list).

Then run the following (You should be able to just copy and paste the below into a
terminal and hit return -- the protoc compiler runs fast):

  UNIX_PROTO_DIR=src/main/protobuf
  JAVA_DIR=src/main/java/
  mkdir -p $JAVA_DIR 2> /dev/null
  if which cygpath 2> /dev/null; then
    PROTO_DIR=`cygpath --windows $UNIX_PROTO_DIR`
    JAVA_DIR=`cygpath --windows $JAVA_DIR`
  else
    PROTO_DIR=$UNIX_PROTO_DIR
  fi
  # uncomment the next line if you want to remove before generating
  # rm -fr $JAVA_DIR/org/apache/hadoop/hbase/protobuf/generated
  for PROTO_FILE in $UNIX_PROTO_DIR/*.proto
  do
    protoc -I$PROTO_DIR --java_out=$JAVA_DIR $PROTO_FILE
  done

After you've done the above, check it in and then check it in (or post a patch
on a JIRA with your definition file changes and the generated files).

Optionally, you can uncomment the hadoop-maven-plugins plugin in hbase-protocol/pom.xml.
This plugin will generate for the classes during the build. Once again, you will need protocol buffers
to be installed on your build machine (https://developers.google.com/protocol-buffers)

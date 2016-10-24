Please read carefully as the 'menu options' have changed.

This module has proto files used by core. These protos
overlap with protos that are used by coprocessor endpoints
(CPEP) in the module hbase-protocol. So the core versions have
a different name, the generated classes are relocated
-- i.e. shaded -- to a new location; they are moved from
org.apache.hadoop.hbase.* to org.apache.hadoop.hbase.shaded.

This module also includes the protobuf that hbase core depends
on again relocated to live at an offset of
org.apache.hadoop.hbase.shaded so as to avoid clashes with other
versions of protobuf resident on our CLASSPATH included,
transitively or otherwise, by dependencies: i.e. the shaded
protobuf Message class is at
org.apache.hadoop.hbase.shaded.com.google.protobuf.Message
rather than at com.google.protobuf.Message.

Finally, this module also includes patches applied on top of
protobuf to add functionality not yet in protobuf that we
need now.

The shaded generated java files, including the patched protobuf
source files are all checked in.

If you make changes to protos, to the protobuf version or to
the patches you want to apply to protobuf, you must rerun this
step.

First ensure that the appropriate protobuf protoc tool is in
your $PATH as in:

 $ export PATH=~/bin/protobuf-3.1.0/src:$PATH

.. or pass -Dprotoc.path=PATH_TO_PROTOC when running
the below mvn commands. You may need to download protobuf and
build protoc first.

Run:

 $ mvn install -Dcompile-protobuf

or

 $ mvn install -Pcompille-protobuf

to build and trigger the special generate-shaded-classes
profile. When finished, the content of
src/main/java/org/apache/hadoop/hbase/shaded will have
been updated. Make sure all builds and then carefully
check in the changes. Files may have been added or removed
by the steps above.

If you have patches for the protobuf, add them to
src/main/patches directory. They will be applied after
protobuf is shaded and unbundled into src/main/java.

See the pom.xml under the generate-shaded-classes profile
for more info on how this step works.

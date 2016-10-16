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

Below we describe how to generate the java files for this
module. Run this step any time you change the proto files
in this module or if you change the protobuf version. If you
add a new file, be sure to add mention of the proto in the
pom.xml (scroll till you see the listing of protos to consider).

First ensure that the appropriate protobuf protoc tool is in
your $PATH as in:

 $ export PATH=~/bin/protobuf-3.1.0/src:$PATH

.. or pass -Dprotoc.path=PATH_TO_PROTOC when running
the below mvn commands. You may need to download protobuf and
build protoc first.

Run:

 $ mvn install -Dgenerate-shaded-classes

or

 $ mvn install -Pgenerate-shaded-classes

to build and trigger the special generate-shaded-classes
profile. When finished, the content of
src/main/java/org/apache/hadoop/hbase/shaded will have
been updated. Check in the changes.

See the pom.xml under the generate-shaded-classes profile
for more info on how this step works.

Please read carefully as the 'menu options' have changed.
What you do in here is not what you do elsewhere to generate
proto java files.

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

If you make changes to protos, to the protobuf version or to
the patches you want to apply to protobuf, you must rerun the
below step and then check in what it generated:

 $ mvn install -Dcompile-protobuf

or

 $ mvn install -Pcompille-protobuf

NOTE: 'install' above whereas other proto generation only needs 'compile'

When finished, the content of src/main/java/org/apache/hadoop/hbase/shaded
will have been updated. Make sure all builds and then carefully
check in the changes. Files may have been added or removed
by the steps above.

The protobuf version used internally by hbase differs from what
is used over in the CPEP hbase-protocol module but mvn takes care
of ensuring we have the right protobuf in place so you don't have to.

If you have patches for the protobuf, add them to
src/main/patches directory. They will be applied after
protobuf is shaded and unbundled into src/main/java.

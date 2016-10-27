ON PROTOBUFS
This maven module has  protobuf definition files ('.protos') used by hbase
Coprocessor Endpoints that ship with hbase core including tests. Coprocessor
Endpoints are meant to be standalone, independent code not reliant on hbase
internals. They define their Service using protobuf. The protobuf version
they use can be distinct from that used by HBase internally since HBase started
shading its protobuf references. Endpoints have no access to the shaded protobuf
hbase uses. They do have access to the content of hbase-protocol but avoid using
as much of this as you can as it is liable to change.

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

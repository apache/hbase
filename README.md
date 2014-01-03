# hbase-native-client

Native client for HBase 0.96

This is a C  library that implements a
HBase client.  It's thread safe and libEv
based.


## Design Philosphy

Synchronous and Async versions will both be built
on the same foundation. The core foundation will
be C++.  External users wanting a C library will
have to choose either async or sync.  These
libraries will be thin veneers ontop of the C++.

We should try and follow pthreads example as much
as possible:

* Consistent naming.
* Opaque pointers as types so that binary compat is easy.
* Simple setup when the defaults are good.
* Attr structs when lots of paramters could be needed.


## Naming
All public C files will start with hbase_*.{h, cc}.  This
is to keep naming conflicts to a minimum. Anything without
the hbase_ prefix is assumed to be implementation private.

All C apis and typedefs will be prefixed with hb_.

All typedefs end with _t.

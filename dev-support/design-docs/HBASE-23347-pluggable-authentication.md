<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Pluggable Authentication for HBase RPCs

## Background

As a distributed database, HBase must be able to authenticate users and HBase
services across an untrusted network. Clients and HBase services are treated
equivalently in terms of authentication (and this is the only time we will
draw such a distinction).

There are currently three modes of authentication which are supported by HBase
today via the configuration property `hbase.security.authentication`

1. `SIMPLE`
2. `KERBEROS`
3. `TOKEN`

`SIMPLE` authentication is effectively no authentication; HBase assumes the user
is who they claim to be. `KERBEROS` authenticates clients via the KerberosV5
protocol using the GSSAPI mechanism of the Java Simple Authentication and Security
Layer (SASL) protocol. `TOKEN` is a username-password based authentication protocol
which uses short-lived passwords that can only be obtained via a `KERBEROS` authenticated
request. `TOKEN` authentication is synonymous with Hadoop-style [Delegation Tokens](https://steveloughran.gitbooks.io/kerberos_and_hadoop/content/sections/hadoop_tokens.html#delegation-tokens). `TOKEN` authentication uses the `DIGEST-MD5`
SASL mechanism.

[SASL](https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html)
is a library which specifies a network protocol that can authenticate a client
and a server using an arbitrary mechanism. SASL ships with a [number of mechanisms](https://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xhtml)
out of the box and it is possible to implement custom mechanisms. SASL is effectively
decoupling an RPC client-server model from the mechanism used to authenticate those
requests (e.g. the RPC code is identical whether username-password, Kerberos, or any
other method is used to authenticate the request).

RFC's define what [SASL mechanisms exist](https://www.iana.org/assignments/sasl-mechanisms/sasl-mechanisms.xml),
but what RFC's define are a superset of the mechanisms that are
[implemented in Java](https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html#SUN).
This document limits discussion to SASL mechanisms in the abstract, focusing on those which are well-defined and
implemented in Java today by the JDK itself. However, it is completely possible that a developer can implement
and register their own SASL mechanism. Writing a custom mechanism is outside of the scope of this document, but
not outside of the realm of possibility.

The `SIMPLE` implementation does not use SASL, but instead has its own RPC logic
built into the HBase RPC protocol. `KERBEROS` and `TOKEN` both use SASL to authenticate,
relying on the `Token` interface that is intertwined with the Hadoop `UserGroupInformation`
class. SASL decouples an RPC from the mechanism used to authenticate that request.

## Problem statement

Despite HBase already shipping authentication implementations which leverage SASL,
it is (effectively) impossible to add a new authentication implementation to HBase. The
use of the `org.apache.hadoop.hbase.security.AuthMethod` enum makes it impossible
to define a new method of authentication. Also, the RPC implementation is written
to only use the methods that are expressly shipped in HBase. Adding a new authentication
method would require copying and modifying the RpcClient implementation, in addition
to modifying the RpcServer to invoke the correct authentication check.

While it is possible to add a new authentication method to HBase, it cannot be done
cleanly or sustainably. This is what is meant by "impossible".

## Proposal

HBase should expose interfaces which allow for pluggable authentication mechanisms
such that HBase can authenticate against external systems. Because the RPC implementation
can already support SASL, HBase can standardize on SASL, allowing any authentication method
which is capable of using SASL to negotiate authentication. `KERBEROS` and `TOKEN` methods
will naturally fit into these new interfaces, but `SIMPLE` authentication will not (see the following
chapter for a tangent on SIMPLE authentication today)

### Tangent: on SIMPLE authentication

`SIMPLE` authentication in HBase today is treated as a special case. My impression is that
this stems from HBase not originally shipping an RPC solution that had any authentication.

Re-implementing `SIMPLE` authentication such that it also flows through SASL (e.g. via
the `PLAIN` SASL mechanism) would simplify the HBase codebase such that all authentication
occurs via SASL. This was not done for the initial implementation to reduce the scope
of the changeset. Changing `SIMPLE` authentication to use SASL may result in some
performance impact in setting up a new RPC. The same conditional logic to determine
`if (sasl) ... else SIMPLE` logic is propagated in this implementation.

## Implementation Overview

HBASE-23347 includes a refactoring of HBase RPC authentication where all current methods
are ported to a new set of interfaces, and all RPC implementations are updated to use
the new interfaces. In the spirit of SASL, the expectation is that users can provide
their own authentication methods at runtime, and HBase should be capable of negotiating
a client who tries to authenticate via that custom authentication method. The implementation
refers to this "bundle" of client and server logic as an "authentication provider".

### Providers

One authentication provider includes the following pieces:

1. Client-side logic (providing a credential)
2. Server-side logic (validating a credential from a client)
3. Client selection logic to choose a provider (from many that may be available)

A provider's client and server side logic are considered to be one-to-one. A `Foo` client-side provider
should never be used to authenticate against a `Bar` server-side provider.

We do expect that both clients and servers will have access to multiple providers. A server may
be capable of authenticating via methods which a client is unaware of. A client may attempt to authenticate
against a server which the server does not know how to process. In both cases, the RPC
should fail when a client and server do not have matching providers. The server identifies
client authentication mechanisms via a `byte authCode` (which is already sent today with HBase RPCs).

A client may also have multiple providers available for it to use in authenticating against
HBase. The client must have some logic to select which provider to use. Because we are
allowing custom providers, we must also allow a custom selection logic such that the
correct provider can be chosen. This is a formalization of the logic already present
in `org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector`.

To enable the above, we have some new interfaces to support the user extensibility:

1. `interface SaslAuthenticationProvider`
2. `interface SaslClientAuthenticationProvider extends SaslAuthenticationProvider`
3. `interface SaslServerAuthenticationProvider extends SaslAuthenticationProvider`
4. `interface AuthenticationProviderSelector`

The `SaslAuthenticationProvider` shares logic which is common to the client and the
server (though, this is up to the developer to guarantee this). The client and server
interfaces each have logic specific to the HBase RPC client and HBase RPC server
codebase, as their name implies. As described above, an implementation
of one `SaslClientAuthenticationProvider` must match exactly one implementation of
`SaslServerAuthenticationProvider`. Each Authentication Provider implementation is
a singleton and is intended to be shared across all RPCs. A provider selector is
chosen per client based on that client's configuration.

A client authentication provider is uniquely identified among other providers
by the following characteristics:

1. A name, e.g. "KERBEROS", "TOKEN"
2. A byte (a value between 0 and 255)

In addition to these attributes, a provider also must define the following attributes:

3. The SASL mechanism being used.
4. The Hadoop AuthenticationMethod, e.g. "TOKEN", "KERBEROS", "CERTIFICATE"
5. The Token "kind", the name used to identify a TokenIdentifier, e.g. `HBASE_AUTH_TOKEN`

It is allowed (even expected) that there may be multiple providers that use `TOKEN` authentication.

N.b. Hadoop requires all `TokenIdentifier` implements to have a no-args constructor and a `ServiceLoader`
entry in their packaging JAR file (e.g. `META-INF/services/org.apache.hadoop.security.token.TokenIdentifier`).
Otherwise, parsing the `TokenIdentifier` on the server-side end of an RPC from a Hadoop `Token` will return
`null` to the caller (often, in the `CallbackHandler` implementation).

### Factories

To ease development with these unknown set of providers, there are two classes which
find, instantiate, and cache the provider singletons.

1. Client side: `class SaslClientAuthenticationProviders`
2. Server side: `class SaslServerAuthenticationProviders`

These classes use [Java ServiceLoader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html)
to find implementations available on the classpath. The provided HBase implementations
for the three out-of-the-box implementations all register themselves via the `ServiceLoader`.

Each class also enables providers to be added via explicit configuration in hbase-site.xml.
This enables unit tests to define custom implementations that may be toy/naive/unsafe without
any worry that these may be inadvertently deployed onto a production HBase cluster.

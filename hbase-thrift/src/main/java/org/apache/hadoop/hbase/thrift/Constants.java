/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.thrift;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrift related constants
 */
@InterfaceAudience.Private
public final class Constants {
  private Constants(){}

  public static final int DEFAULT_HTTP_MAX_HEADER_SIZE = 64 * 1024; // 64k

  public static final String SERVER_TYPE_CONF_KEY =
      "hbase.regionserver.thrift.server.type";

  public static final String COMPACT_CONF_KEY = "hbase.regionserver.thrift.compact";
  public static final boolean COMPACT_CONF_DEFAULT = false;

  public static final String FRAMED_CONF_KEY = "hbase.regionserver.thrift.framed";
  public static final boolean FRAMED_CONF_DEFAULT = false;

  public static final String MAX_FRAME_SIZE_CONF_KEY =
      "hbase.regionserver.thrift.framed.max_frame_size_in_mb";
  public static final int MAX_FRAME_SIZE_CONF_DEFAULT = 2;

  public static final String COALESCE_INC_KEY = "hbase.regionserver.thrift.coalesceIncrement";
  public static final String USE_HTTP_CONF_KEY = "hbase.regionserver.thrift.http";

  public static final String HTTP_MIN_THREADS_KEY = "hbase.thrift.http_threads.min";
  public static final int HTTP_MIN_THREADS_KEY_DEFAULT = 2;

  public static final String HTTP_MAX_THREADS_KEY = "hbase.thrift.http_threads.max";
  public static final int HTTP_MAX_THREADS_KEY_DEFAULT = 100;

  // ssl related configs
  public static final String THRIFT_SSL_ENABLED_KEY = "hbase.thrift.ssl.enabled";
  public static final String THRIFT_SSL_KEYSTORE_STORE_KEY = "hbase.thrift.ssl.keystore.store";
  public static final String THRIFT_SSL_KEYSTORE_PASSWORD_KEY =
      "hbase.thrift.ssl.keystore.password";
  public static final String THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY
      = "hbase.thrift.ssl.keystore.keypassword";
  public static final String THRIFT_SSL_EXCLUDE_CIPHER_SUITES_KEY =
      "hbase.thrift.ssl.exclude.cipher.suites";
  public static final String THRIFT_SSL_INCLUDE_CIPHER_SUITES_KEY =
      "hbase.thrift.ssl.include.cipher.suites";
  public static final String THRIFT_SSL_EXCLUDE_PROTOCOLS_KEY =
      "hbase.thrift.ssl.exclude.protocols";
  public static final String THRIFT_SSL_INCLUDE_PROTOCOLS_KEY =
      "hbase.thrift.ssl.include.protocols";
  public static final String THRIFT_SSL_KEYSTORE_TYPE_KEY =
    "hbase.thrift.ssl.keystore.type";
  public static final String THRIFT_SSL_KEYSTORE_TYPE_DEFAULT =
    "jks";


  public static final String THRIFT_SUPPORT_PROXYUSER_KEY = "hbase.thrift.support.proxyuser";

  //kerberos related configs
  public static final String THRIFT_DNS_INTERFACE_KEY = "hbase.thrift.dns.interface";
  public static final String THRIFT_DNS_NAMESERVER_KEY = "hbase.thrift.dns.nameserver";
  public static final String THRIFT_KERBEROS_PRINCIPAL_KEY = "hbase.thrift.kerberos.principal";
  public static final String THRIFT_KEYTAB_FILE_KEY = "hbase.thrift.keytab.file";
  public static final String THRIFT_SPNEGO_PRINCIPAL_KEY = "hbase.thrift.spnego.principal";
  public static final String THRIFT_SPNEGO_KEYTAB_FILE_KEY = "hbase.thrift.spnego.keytab.file";

  /**
   * Amount of time in milliseconds before a server thread will timeout
   * waiting for client to send data on a connected socket. Currently,
   * applies only to TBoundedThreadPoolServer
   */
  public static final String THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY =
      "hbase.thrift.server.socket.read.timeout";
  public static final int THRIFT_SERVER_SOCKET_READ_TIMEOUT_DEFAULT = 60000;


  /**
   * Thrift quality of protection configuration key. Valid values can be:
   * auth-conf: authentication, integrity and confidentiality checking
   * auth-int: authentication and integrity checking
   * auth: authentication only
   *
   * This is used to authenticate the callers and support impersonation.
   * The thrift server and the HBase cluster must run in secure mode.
   */
  public static final String THRIFT_QOP_KEY = "hbase.thrift.security.qop";

  public static final String BACKLOG_CONF_KEY = "hbase.regionserver.thrift.backlog";
  public static final int BACKLOG_CONF_DEAFULT = 0;

  public static final String BIND_CONF_KEY = "hbase.regionserver.thrift.ipaddress";
  public static final String DEFAULT_BIND_ADDR = "0.0.0.0";

  public static final String PORT_CONF_KEY = "hbase.regionserver.thrift.port";
  public static final int DEFAULT_LISTEN_PORT = 9090;

  public static final String THRIFT_HTTP_ALLOW_OPTIONS_METHOD =
      "hbase.thrift.http.allow.options.method";
  public static final boolean THRIFT_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT = false;

  public static final String THRIFT_INFO_SERVER_PORT = "hbase.thrift.info.port";
  public static final int THRIFT_INFO_SERVER_PORT_DEFAULT = 9095;

  public static final String THRIFT_INFO_SERVER_BINDING_ADDRESS = "hbase.thrift.info.bindAddress";
  public static final String THRIFT_INFO_SERVER_BINDING_ADDRESS_DEFAULT = "0.0.0.0";

  public static final String THRIFT_QUEUE_SIZE = "hbase.thrift.queue.size";
  public static final int THRIFT_QUEUE_SIZE_DEFAULT = Integer.MAX_VALUE;

  public static final String THRIFT_SELECTOR_NUM = "hbase.thrift.selector.num";

  public static final String THRIFT_FILTERS = "hbase.thrift.filters";

  // Command line options

  public static final String READ_TIMEOUT_OPTION = "readTimeout";
  public static final String MIN_WORKERS_OPTION = "minWorkers";
  public static final String MAX_WORKERS_OPTION = "workers";
  public static final String MAX_QUEUE_SIZE_OPTION = "queue";
  public static final String SELECTOR_NUM_OPTION = "selectors";
  public static final String KEEP_ALIVE_SEC_OPTION = "keepAliveSec";
  public static final String BIND_OPTION = "bind";
  public static final String COMPACT_OPTION = "compact";
  public static final String FRAMED_OPTION = "framed";
  public static final String PORT_OPTION = "port";
  public static final String INFOPORT_OPTION = "infoport";

  //for thrift2 server
  public static final String READONLY_OPTION ="readonly";

  public static final String THRIFT_READONLY_ENABLED = "hbase.thrift.readonly";
  public static final boolean THRIFT_READONLY_ENABLED_DEFAULT = false;

  public static final String HBASE_THRIFT_CLIENT_SCANNER_CACHING =
      "hbase.thrift.client.scanner.caching";

  public static final int HBASE_THRIFT_CLIENT_SCANNER_CACHING_DEFAULT = 20;

  public static final String HBASE_THRIFT_SERVER_NAME = "hbase.thrift.server.name";
  public static final String HBASE_THRIFT_SERVER_PORT = "hbase.thrift.server.port";

  public static final String HBASE_THRIFT_CLIENT_BUIDLER_CLASS =
      "hbase.thrift.client.builder.class";


}

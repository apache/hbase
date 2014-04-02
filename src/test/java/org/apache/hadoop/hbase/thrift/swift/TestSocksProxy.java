package org.apache.hadoop.hbase.thrift.swift;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.thrift.ThriftClientObjectFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.facebook.swift.service.ThriftClientConfig;

public class TestSocksProxy {
  @Test
  public void test() {
    ThriftClientConfig config = new ThriftClientConfig();
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.SWIFT_CLIENT_SOCKS_PROXY_HOST_AND_PORT,
        "localhost:8091");
    ThriftClientObjectFactory.setSocksProxy(config, conf);
    assertEquals("Hostname parsed correctly",
        config.getSocksProxy().getHostText(), "localhost");
    assertEquals("Port number parse correctly",
        config.getSocksProxy().getPort(), 8091);

    config = new ThriftClientConfig();
    conf.set(HConstants.SWIFT_CLIENT_SOCKS_PROXY_HOST_AND_PORT,
        "localhost");
    ThriftClientObjectFactory.setSocksProxy(config, conf);
    assertNull("Null values expected for the host and port. "
        + config.getSocksProxy(), config.getSocksProxy());
  }
}

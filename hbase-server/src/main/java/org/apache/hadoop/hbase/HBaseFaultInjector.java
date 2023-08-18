package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;

@InterfaceAudience.Private
public class HBaseFaultInjector {
  private static HBaseFaultInjector instance = new HBaseFaultInjector();

  public static HBaseFaultInjector get() {
    return instance;
  }

  public static void set(HBaseFaultInjector injector) {
    instance = injector;
  }

  public void injectIOException() throws IOException {}

  public void killTaskNode(ServerName name) {}

  public void collectFNFException() {

  }
}

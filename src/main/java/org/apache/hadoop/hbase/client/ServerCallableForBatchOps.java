package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;

/**
 * A partial implementation of ServerCallable, used for batch operations
 * spanning multiple regions/rows.
 * @param <T> the class that the ServerCallable handles
 */
public abstract class ServerCallableForBatchOps<T> extends ServerCallable<T> {
  HServerAddress address;

  public ServerCallableForBatchOps(HConnection connection, HServerAddress address,
      HBaseRPCOptions options) {
    super(connection, null, null, options);
    this.address = address;
  }

  @Override
  public void instantiateRegionLocation(boolean reload) throws IOException {
     // we don't need to locate the region, since we have been given the address of the
     // server. But, let us store the information in this.location, so that
     // we can handle failures (i.e. clear cache) if we fail to connect to the RS.
     this.location = new HRegionLocation(null, address);
  }

  @Override
  public void instantiateServer() throws IOException {
    server = connection.getHRegionConnection(address, options);
  }
}

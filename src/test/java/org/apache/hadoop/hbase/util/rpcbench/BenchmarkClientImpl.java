package org.apache.hadoop.hbase.util.rpcbench;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.ProfilingData;

public abstract class BenchmarkClientImpl implements BenchmarkClient {

  private static final Log LOG = LogFactory.getLog(BenchmarkClientImpl.class);
  protected HTable htable = null;
  @Override
  public Put createRandomPut(int rowLength, byte[] family, int qualLength,
      int valueLength) {
    Random rand = new Random();
    byte[] row = new byte[rowLength];
    byte[] qual = new byte[qualLength];
    byte[] value = new byte[valueLength];
    rand.nextBytes(row);
    rand.nextBytes(qual);
    rand.nextBytes(value);
    return this.createPut(row, family, qual, value);
  }
  // Performing a get through thrift
  @Override
  public Result executeGet(Get get) {
    Result r = null;
    try {
      r = this.htable.get(get);
    } catch (IOException e) {
      LOG.debug("Unable to perform get");
      e.printStackTrace();
    }
    return r;
  }

  // Performing a put through hadoop rpc.
  @Override
  public void executePut(Put put) {
    try {
      this.htable.put(put);
    } catch (IOException e) {
      LOG.debug("Unable to perform put");
      e.printStackTrace();
    }
  }

  @SuppressWarnings("deprecation")
  public Get createGet(byte[] row, byte[] family, byte[] qual) {
    Get g = new Get(row);
    g.addColumn(family, qual);
    return g;
  }

  public Put createPut(byte[] row, byte[] family, byte[] qual, byte[] value) {
    Put p = new Put(row);
    p.add(family, qual, value);
    return p;
  }

  @Override
  public List<Result> executeScan(Scan scan) {
    throw new NotImplementedException();
  }

  @Override
  public Scan createScan(byte[] row, byte[] family, int nbRows) {
    throw new NotImplementedException();
  }

  @Override
  public Result[] executeMultiGet(List<Get> gets) {
    try {
      return this.htable.batchGet(gets);
    } catch (IOException e) {
      LOG.debug("Unable to perform put");
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void executeMultiPut(List<Put> puts) {
    try {
      this.htable.put(puts);
    } catch (IOException e) {
      LOG.debug("Unable to perform put");
      e.printStackTrace();
    }
  }

  public void printProfilingData() {
    if (htable.getProfiling()) {
      ProfilingData data = htable.getProfilingData();
      LOG.debug(data.toPrettyString());
    }
  }

  public void setProfilingData(boolean flag) {
    this.htable.setProfiling(flag);
  }
}

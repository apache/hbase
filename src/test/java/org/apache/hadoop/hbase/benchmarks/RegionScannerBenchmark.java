package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This test benchmarks the performance of scanning from the block cache when 
 * run as a library. Note that this will not have the overheads of reporting 
 * metrics, etc. This is the theoretical throughput the RegionScanner can 
 * achieve, so this is a bound for the scan throughput.
 */
public class RegionScannerBenchmark {
  public static final Log LOG = LogFactory.getLog(RegionScannerBenchmark.class);
  private Configuration conf;
  private FileSystem fs;
  private Path hbaseRootDir = null;
  private Path oldLogDir;
  private Path logDir;
  
  public RegionScannerBenchmark() throws IOException {
    conf = HBaseConfiguration.create();
    fs = FileSystem.get(conf);
    this.hbaseRootDir = new Path(this.conf.get(HConstants.HBASE_DIR));
    this.oldLogDir = 
      new Path(this.hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    this.logDir = new Path(this.hbaseRootDir, HConstants.HREGION_LOGDIR_NAME);
    
    reinitialize();
  }
  
  public void reinitialize() throws IOException {
    if (fs.exists(this.hbaseRootDir)) {
      fs.delete(this.hbaseRootDir, true);
    }
    Path rootdir = fs.makeQualified(new Path(conf.get(HConstants.HBASE_DIR)));
    fs.mkdirs(rootdir);    
  }
  
  public void runBenchmark() throws Throwable {
    String tableNameStr = "RegionScannerBenchmark";
    HRegion region = createAndbulkLoadHRegion(tableNameStr, 50, 1000000);
    
    // warm block cache and jvm jit compilation
    for (int i = 0; i < 20; i++) {
      scanHRegion(true, region);
    }
  }

  public void scanHRegion(boolean printStats, HRegion region) 
  throws IOException {
    // create the scan object with the right params
    Scan scan = new Scan();
    scan.setMaxVersions(1);

    // create the RegionScanner object
    InternalScanner scanner = region.getScanner(scan);
    
    // do the scan
    long numKVs = 0;
    long numBytes = 0;
    List<KeyValue> results = new ArrayList<KeyValue>();
    long t1 = System.currentTimeMillis();
    while (scanner.next(results) || results.size() > 0) {
      for (KeyValue kv : results) {
        numKVs++;
        numBytes += kv.getLength();
      }
      results.clear();
    }
    long t2 = System.currentTimeMillis();
    scanner.close();
    
    if (printStats) {
      double numBytesInMB = numBytes * 1.0 / (1024 * 1024);
      double rate = numBytesInMB * (1000 * 1.0 / (t2 - t1));
      System.out.println(
          "Scan: from region scanner" +
          ", kvs = " + numKVs +
          ", bytes = " + String.format("%1$,.2f", numBytesInMB) + " MB" +
          ", time = " + (t2 - t1) + " ms" +
          ", rate = " + String.format("%1$,.2f", rate) + "MB/s"
          );
    }
  }
  
  public HRegion createAndbulkLoadHRegion(String tableNameStr, 
      int kvSize, int numKVs) throws IOException {
    // cleanup old data
    Path basedir = new Path(this.hbaseRootDir, tableNameStr);
    deleteDir(basedir);
    
    // setup the region
    HLog wal = createWAL(this.conf);
    HTableDescriptor htd = new HTableDescriptor(tableNameStr);
    HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
    htd.addFamily(a);
    HRegionInfo hri = new HRegionInfo(htd, null, null, false);
    HRegion region = HRegion.openHRegion(hri, basedir, wal, this.conf);

    // bulk load some data
    Path f =  new Path(basedir, "hfile");
    HFile.Writer writer =
      HFile.getWriterFactoryNoCache(conf).withPath(fs, f).create();
    byte [] family = 
      hri.getTableDesc().getFamilies().iterator().next().getName();
    byte [] row = Bytes.toBytes(tableNameStr);
    byte [] value = new byte[kvSize];
    (new Random()).nextBytes(value);
    for (int i = 0; i < numKVs; i++) {
      writer.append(new KeyValue(row, family, Bytes.toBytes(i), 
          System.currentTimeMillis(), value));
    }
    writer.close();
    region.bulkLoadHFile(f.toString(), family);
    return region;
  }

  private void deleteDir(final Path p) throws IOException {
    if (this.fs.exists(p)) {
      if (!this.fs.delete(p, true)) {
        throw new IOException("Failed remove of " + p);
      }
    }
  }
  private HLog createWAL(final Configuration c) throws IOException {
    HLog wal = new HLog(FileSystem.get(c), logDir, oldLogDir, c, null);
    return wal;
  }

  public static void main(String[] args) throws Throwable {
    RegionScannerBenchmark benchmark = new RegionScannerBenchmark();
    benchmark.runBenchmark();
  }
}

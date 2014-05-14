package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.TestHFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFile.Writer;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCompression extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestHFile.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String ROOT_DIR =
      TEST_UTIL.getTestDir("TestHFile").toString();
  private final int minBlockSize = 512;
  private static String localFormatter = "%010d";
  private static CacheConfig cacheConf = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private void writeRecords(Writer writer) throws IOException {
    writeSomeRecords(writer, 0, 100);
    writer.close();
  }

  // write some records into the tfile
  // write them twice
  private int writeSomeRecords(Writer writer, int start, int n)
      throws IOException {
    String value = "value";
    for (int i = start; i < (start + n); i++) {
      String key = String.format(localFormatter, Integer.valueOf(i));
      writer.append(Bytes.toBytes(key), Bytes.toBytes(value + key));
    }
    return (start + n);
  }

  public void testCompressionAlgorithmThreadSafety() throws IOException{
    // Create some HFiles
    fs.mkdirs(new Path(ROOT_DIR));
    int numFiles = 100;
    String codec = "gz";
    if (cacheConf == null) cacheConf = new CacheConfig(conf);
    List<Path> createdHFiles = new ArrayList<Path>();
    for (int i=0; i<numFiles; i++) {
      Path p = StoreFile.getUniqueFile(fs, new Path(ROOT_DIR));
      createdHFiles.add(p);
      FSDataOutputStream fout = createFSOutput(p);
      conf.setInt(HFile.FORMAT_VERSION_KEY, 2);
      Writer writer = HFile.getWriterFactory(conf, cacheConf)
          .withOutputStream(fout)
          .withBlockSize(minBlockSize)
          .withCompression(codec)
          .create();
      LOG.info(writer);
      writeRecords(writer);
      fout.close();
    }
    LOG.debug("Clearing the codecs");
    for (Algorithm a : Algorithm.values()) {
      a.deleteCodec();
    }
    try {
      List<Reader> lst = loadHFileReaders(createdHFiles);
    } catch (IOException e) {
      throw e;
    } catch (RuntimeException e) {
      // Unsafe execution of the threads might cause RuntimeException.
      assertTrue(false);
    }
  }

  private FSDataOutputStream createFSOutput(Path name) throws IOException {
    if (fs.exists(name)) fs.delete(name, true);
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  public static ThreadPoolExecutor getReaderCreatorThreadPool(int maxThreads,
      final String threadNamePrefix) {
    ThreadPoolExecutor readerCreatorThreadPool = Threads
        .getBoundedCachedThreadPool(maxThreads, 30L, TimeUnit.SECONDS,
            new ThreadFactory() {
              private int count = 1;

              public Thread newThread(Runnable r) {
                Thread t = new Thread(r, threadNamePrefix + "-" + count++);
                t.setDaemon(true);
                return t;
              }
            });
    return readerCreatorThreadPool;
  }

  private List<Reader> loadHFileReaders(List<Path> files) throws IOException {
    // initialize the thread pool for opening store files in parallel..
    ThreadPoolExecutor ReaderCreatorThreadPool =
        getReaderCreatorThreadPool(files.size(), "ReaderCreatorPool");
    CompletionService<Reader> completionService =
      new ExecutorCompletionService<Reader>(ReaderCreatorThreadPool);

    int totalValidHFile = 0;
    for (int i = 0; files != null && i < files.size(); i++) {
      final Path p = files.get(i);
      completionService.submit(new Callable<Reader>() {
        public Reader call() throws IOException {
          return HFile.createReader(fs, p, cacheConf);
        }
      });
      totalValidHFile++;
    }
    List<Reader> ret = new ArrayList<Reader>();
    try {
      for (int i = 0; i < totalValidHFile; i++) {
        Future<Reader> future = completionService.take();
        Reader hfileReader = future.get();
        hfileReader.loadFileInfo();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Created HFileReader for " + hfileReader.getName());
        }
        ret.add(hfileReader);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    } finally {
      ReaderCreatorThreadPool.shutdownNow();
    }
    return ret;
  }
}

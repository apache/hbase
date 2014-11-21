package org.apache.hadoop.hbase.consensus.log;

import org.apache.hadoop.hbase.HConstants;

import java.io.File;
import java.io.IOException;

/**
 * This is a special log file used to denote the seed Index for the transaction
 * log manager.
 */
public class SeedLogFile extends ReadOnlyLog {

  public SeedLogFile(final File file) {
    super(file, HConstants.SEED_TERM, getInitialSeedIndex(file));
  }

  /**
   * Remove and close the registered reader from the reader map by the session key
   * @param sessionKey
   */
  public void removeReader(String sessionKey) throws IOException {
    // Do-nothing
  }

  /**
   * Close all the LogReader instances and delete the file
   * @throws IOException
   */
  @Override
  public void closeAndDelete() throws IOException {
    super.closeAndDelete();
  }

  @Override public long getTxnCount() {
    return lastIndex - initialIndex + 1;
  }

  public static boolean isSeedFile(final File f) {
    return isSeedFile(f.getName());
  }

  public static boolean isSeedFile(String fileName) {
    String[] split = fileName.split("_");
    return split[0].equals("-2");
  }

  public static long getInitialSeedIndex(final File f) {
    final String[] split = f.getName().split("_");
    return Long.parseLong(split[1]);
  }
}

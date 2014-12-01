package org.apache.hadoop.hbase.consensus.rmap;

import java.io.IOException;
import java.net.URI;

public class NoSuchRMapException extends IOException {
  public NoSuchRMapException(final URI uri) {
    super("No RMap found with URI " + uri);
  }
}

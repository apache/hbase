package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HDFSReader extends RMapReader {
  protected static final Logger LOG = LoggerFactory.getLogger(HDFSReader.class);

  private Configuration conf;

  public HDFSReader(final Configuration conf) {
    this.conf = conf;
  }

  @Override
  public List<Long> getVersions(URI uri) throws IOException {
    Path path = new Path(getSchemeAndPath(uri));
    FileSystem fs = path.getFileSystem(conf);
    FileStatus[] statuses = fs.globStatus(new Path(path.toString() + ".*"));

    List<Long> versions = new ArrayList<>(statuses.length);
    for (FileStatus status : statuses) {
      long version = getVersionFromPath(status.getPath().toString());
      if (version > 0) {
        versions.add(version);
      }
    }
    Collections.sort(versions);
    return versions;
  }

  @Override
  public URI resolveSymbolicVersion(URI uri) throws URISyntaxException {
    long version = getVersion(uri);
    String schemeAndPath = getSchemeAndPath(uri);

    if (version == RMapReader.CURRENT || version == RMapReader.NEXT) {
      Path link = new Path(String.format("%s.%s", schemeAndPath,
              version == RMapReader.CURRENT ? "CURRENT" : "NEXT"));
      // Resolve to an explicit version, or UNKNOWN
      try {
        Path target = getLinkTarget(link);
        version = target != null ? getVersionFromPath(target.toString()) :
                RMapReader.UNKNOWN;
      } catch (IOException e) {
        LOG.error("Failed to look up version from link:", e);
        version = RMapReader.UNKNOWN;
      }
    }

    if (version > 0) {
      return new URI(String.format("%s?version=%d", schemeAndPath, version));
    }
    return new URI(schemeAndPath);
  }

  @Override
  public String readRMapAsString(final URI uri) throws IOException {
    // Get file status, throws IOException if the path does not exist.
    Path path = getPathWithVersion(uri);
    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);

    long n = status.getLen();
    if (n < 0 || n > MAX_SIZE_BYTES) {
      throw new IOException(String.format("Invalid RMap file size " +
              "(expected between 0 and %d but got %d bytes)",
              MAX_SIZE_BYTES, n));
    }

    byte[] buf = new byte[(int)n];
    FSDataInputStream stream = fs.open(path);
    try {
      stream.readFully(buf);
    } finally {
      stream.close();
    }
    return Bytes.toString(buf);
  }

  public Path getPathWithVersion(final URI uri) throws IOException {
    long version = RMapReader.UNKNOWN;
    try {
      version = getVersion(resolveSymbolicVersion(uri));
    } catch (URISyntaxException e) {
      // Ignore invalid URIs and assume version UNKNOWN
    }

    if (version > 0) {
      return new Path(String.format("%s.%d", getSchemeAndPath(uri), version));
    }
    return new Path(uri.toString());
  }

  private long getVersionFromPath(final String path) {
    String[] tokens = path.split("[\\.]");
    try {
      return Long.parseLong(tokens[tokens.length - 1]);
    } catch (NumberFormatException e) {
      // Skip if token not numerical
    }
    return RMapReader.UNKNOWN;
  }

  private Path getLinkTarget(final Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);

    // The getHardLinkedFiles call is a bit tricky, as it effectively returns
    // all other paths to the inode shared with the given path. In order to
    // guard against erroneous links, only consider those where the paths
    // are the same, up to the version.
    String pathWithoutVersion = path.toString().substring(0,
            path.toString().lastIndexOf('.'));
    /*
TODO: FIXME: Amit: this code works with the internal hdfs. might not work with the
OSS version.

    for (String link : fs.getHardLinkedFiles(path)) {
      if (path.toString().startsWith(pathWithoutVersion) &&
              getVersionFromPath(link) > 0) {
        return new Path(link);
      }
    }
    */
    return null;
  }
}

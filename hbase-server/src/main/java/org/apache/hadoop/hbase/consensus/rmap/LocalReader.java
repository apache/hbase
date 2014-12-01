package org.apache.hadoop.hbase.consensus.rmap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LocalReader extends RMapReader {
  protected static final Logger LOG = LoggerFactory.getLogger(
          LocalReader.class);

  @Override
  public List<Long> getVersions(final URI uri) throws IOException {
    Path path = Paths.get(uri);
    List<Long> versions = new ArrayList<>();

    for (Path match : Files.newDirectoryStream(path.getParent(),
            path.getFileName() + ".*")) {
      long version = getVersionFromPath(match.toString());
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
      Path link = Paths.get(String.format("%s.%s", schemeAndPath,
              version == RMapReader.CURRENT ? "CURRENT" : "NEXT"));
      // Resolve to an explicit version, or UNKNOWN
      try {
        version = getVersionFromPath(Files.readSymbolicLink(link).toString());
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
    Path path = getPathWithVersion(uri);

    long n = Files.size(path);
    if (n < 0 || n > MAX_SIZE_BYTES) {
      throw new IOException(String.format("Invalid RMap file size " +
              "(expected between 0 and %d but got %d bytes)",
              MAX_SIZE_BYTES, n));
    }

    return new String(Files.readAllBytes(path));
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

  private Path getPathWithVersion(final URI uri) {
    long version = RMapReader.UNKNOWN;
    try {
      version = getVersion(resolveSymbolicVersion(uri));
    } catch (URISyntaxException e) {
      // Ignore invalid URIs and assume version UNKNOWN
    }

    if (version > 0) {
      return Paths.get(String.format("%s.%d", uri.getPath(), version));
    }
    return Paths.get(uri);
  }
}

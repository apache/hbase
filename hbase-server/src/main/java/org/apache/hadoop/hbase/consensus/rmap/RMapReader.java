package org.apache.hadoop.hbase.consensus.rmap;

import org.apache.commons.codec.binary.Hex;
//import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public abstract class RMapReader {
  /** Max file sisze of a single file containing a RMap */
  public static long MAX_SIZE_BYTES = 16 * 1024 * 1204; // 16 MB

  /** RMap version special values */
  public static long NEXT = -2;
  public static long CURRENT = -1;
  public static long UNKNOWN = 0;

  /**
   * Return a naturally sorted list of available versions of a given RMap URI.
   *
   * @param uri URI of the RMap
   * @return a naturally sorted list of versions of the given RMap URI
   * @throws IOException if an exception occurs while reading versions
   */
  public abstract List<Long> getVersions(final URI uri) throws IOException;

  /**
   * Resolve a URI containing a symbolic version into a URI with an absolute
   * value which can be requested from the reader.
   *
   * @param uri URI containing a symbolic version
   * @return a URI containing an absolute version
   * @throws URISyntaxException if the given URI is malformed
   */
  public abstract URI resolveSymbolicVersion(final URI uri)
          throws URISyntaxException;

  /**
   * Return the contents of the RMap at given URI as a string.
   *
   * @param uri URI of the RMap
   * @return contents of the RMap as String
   * @throws IOException if an exception occurs while reading the RMap
   */
  public abstract String readRMapAsString(final URI uri) throws IOException;

  /**
   * Return the version number of the RMap specified in the given URI.
   *
   * @param uri URI of the RMap
   * @return the version number of the RMap or 0 if no version was found
   */
  public static long getVersion(final URI uri) {
    for (NameValuePair param : URLEncodedUtils.parse(uri, "UTF-8")) {
      if (param.getName().equals("version")) {
        switch (param.getValue().toUpperCase()) {
          case "NEXT":
            return NEXT;
          case "CURRENT":
            return CURRENT;
          default:
            try {
              return Long.parseLong(param.getValue());
            } catch (NumberFormatException e) {
              /* Ignore if NaN */
            }
        }
      }
    }
    return UNKNOWN;
  }

  public static boolean isSymbolicVersion(final URI uri) {
    return getVersion(uri) < 0;
  }

  /**
   * Read and return a {@link RMapJSON} of the RMap at the given URI.
   *
   * @param uri URI of the RMap
   * @return a JSON representation of the RMap
   * @throws IOException if an (possible transient) exception occurs while
   *        reading the RMap
   * @throws RMapException if any other exception occurs while reading the RMap
   */
  public RMapJSON readRMap(final URI uri) throws IOException, RMapException {
    URI nonSymbolicURI;
    try {
      nonSymbolicURI = resolveSymbolicVersion(uri);
      String encodedRMap = readRMapAsString(nonSymbolicURI);
      return new RMapJSON(nonSymbolicURI, new JSONObject(encodedRMap),
              getSignature(encodedRMap));
    } catch (URISyntaxException e) {
      throw new RMapException("URI syntax invalid for RMap: " + uri, e);
    } catch (JSONException e) {
      throw new RMapException(
              "Failed to decode JSON string for RMap: " + uri, e);
    } catch (NoSuchAlgorithmException e) {
      throw new RMapException(
              "Failed to generate signature for RMap: " + uri, e);
    }
  }

  /**
   * Get a MD5 hash of the given string.
   *
   * @param s string to be hashed
   * @return a hex String representation of the hash
   * @throws NoSuchAlgorithmException if MD5 message digest is unavailable
   */
  public static String getSignature(final String s)
          throws NoSuchAlgorithmException {
    return new String(Hex.encodeHex(
            MessageDigest.getInstance("MD5").digest(s.getBytes())));
  }

  /**
   * Get a MD5 hash of the given string.
   *
   * @param s string to be hashed
   * @return a hex String representation of the hash
   * @throws NoSuchAlgorithmException if MD5 message digest is unavailable
   */
  public String getSignature(final URI uri) throws IOException, RMapException {
    URI nonSymbolicURI;
    try {
      nonSymbolicURI = resolveSymbolicVersion(uri);
      String encodedRMap = readRMapAsString(nonSymbolicURI);
      return getSignature(encodedRMap);
    } catch (URISyntaxException e) {
      throw new RMapException("URI syntax invalid for RMap: " + uri, e);
    } catch (NoSuchAlgorithmException e) {
      throw new RMapException(
              "Failed to generate signature for RMap: " + uri, e);
    }
  }

  /**
   * Get the scheme, authority (if present) and path of a given URI as a string.
   * @param uri
   * @return a string containing just the scheme, authority and path
   */
  public static String getSchemeAndPath(final URI uri) {
    return String.format("%s:%s%s", uri.getScheme(),
            uri.getAuthority() != null ?
                    String.format("//%s", uri.getAuthority()) : "",
            uri.getPath());
  }

  /**
   * Get a versioned URI for the RMap with given scheme, path and version.
   * @param schemeAndPath
   * @param version
   * @return a URI of the form [scheme]:[authority]//[path]?version=[version]
   * @throws URISyntaxException
   */
  public static URI getVersionedURI(final String schemeAndPath,
          final long version) throws URISyntaxException {
    String token = "UNKNOWN";

    if (version > 0) {
      token = String.format("%d", version);
    } else if (version == CURRENT) {
      token = "CURRENT";
    } else if (version == NEXT) {
      token = "NEXT";
    }

    return new URI(String.format("%s?version=%s", schemeAndPath, token));
  }

  /**
   * Get a versioned URI for the RMap with given base URI and version. If the
   * given URI already contains a version it is overwritten by the given
   * version.
   * @param uri
   * @param version
   * @return a URI of the form [scheme]:[authority]//[path]?version=[version]
   * @throws URISyntaxException
   */
  public static URI getVersionedURI(final URI uri, final long version)
          throws URISyntaxException {
    return getVersionedURI(getSchemeAndPath(uri), version);
  }

  public long getCurrentVersion(final String schemeAndPath)
          throws URISyntaxException {
    return getVersion(resolveSymbolicVersion(
            getVersionedURI(schemeAndPath, CURRENT)));
  }

  public long getNextVersion(final String schemeAndPath)
          throws URISyntaxException {
    return getVersion(resolveSymbolicVersion(
            getVersionedURI(schemeAndPath, NEXT)));
  }
}

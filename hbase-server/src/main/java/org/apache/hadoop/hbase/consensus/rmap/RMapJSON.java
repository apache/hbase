package org.apache.hadoop.hbase.consensus.rmap;

import org.json.JSONObject;

import java.net.URI;

public class RMapJSON {
  final URI uri;
  final JSONObject rmap;
  final String signature;

  public RMapJSON(final URI uri, final JSONObject rmap,
                  final String signature) {
    this.uri = uri;
    this.rmap = rmap;
    this.signature = signature;
  }

  public long getVersion() {
    return RMapReader.getVersion(uri);
  }

  public URI getURI() {
    return uri;
  }

  public JSONObject getEncodedRMap() {
    return rmap;
  }

  public String getSignature() {
    return signature;
  }
}

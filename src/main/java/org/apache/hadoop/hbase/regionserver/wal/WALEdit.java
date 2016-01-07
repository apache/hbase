/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.Decoder;
import org.apache.hadoop.hbase.codec.Encoder;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.Writable;

/**
 * WALEdit: Used in HBase's transaction log (WAL) to represent
 * the collection of edits (KeyValue objects) corresponding to a
 * single transaction. The class implements "Writable" interface
 * for serializing/deserializing a set of KeyValue items.
 *
 * Previously, if a transaction contains 3 edits to c1, c2, c3 for a row R,
 * the HLog would have three log entries as follows:
 *
 *    <logseq1-for-edit1>:<KeyValue-for-edit-c1>
 *    <logseq2-for-edit2>:<KeyValue-for-edit-c2>
 *    <logseq3-for-edit3>:<KeyValue-for-edit-c3>
 *
 * This presents problems because row level atomicity of transactions
 * was not guaranteed. If we crash after few of the above appends make
 * it, then recovery will restore a partial transaction.
 *
 * In the new world, all the edits for a given transaction are written
 * out as a single record, for example:
 *
 *   <logseq#-for-entire-txn>:<WALEdit-for-entire-txn>
 *
 * where, the WALEdit is serialized as:
 *   <-1, # of edits, <KeyValue>, <KeyValue>, ... >
 * For example:
 *   <-1, 3, <Keyvalue-for-edit-c1>, <KeyValue-for-edit-c2>, <KeyValue-for-edit-c3>>
 *
 * The -1 marker is just a special way of being backward compatible with
 * an old HLog which would have contained a single <KeyValue>.
 *
 * The deserializer for WALEdit backward compatibly detects if the record
 * is an old style KeyValue or the new style WALEdit.
 *
 */
public class WALEdit implements Writable, HeapSize {

  /*
   * The cluster id of the cluster which has consumed the change represented by this class is
   * prefixed with the value of this variable while storing in the scopes variable. This is to
   * ensure that the cluster ids don't interfere with the column family replication settings stored
   * in the scopes. The value is chosen to start with period as the column families can't start with
   * it.
   */
  private static final String PREFIX_CLUSTER_KEY = ".";
  private final int VERSION_2 = -1;

  private final ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);

  /**
   * This variable contains the information of the column family replication settings and contains
   * the clusters that have already consumed the change represented by the object. This overloading
   * of scopes with the consumed clusterids was introduced while porting the fix for HBASE-7709 back
   * to 0.94 release. However, this overloading has been removed in the newer releases(0.95.2+). To
   * check/change the column family settings, please use the getFromScope and putIntoScope methods
   * and for marking/checking if a cluster has consumed the change, please use addCluster,
   * addClusters and getClusters methods.
   */
  private final NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(
      Bytes.BYTES_COMPARATOR);

  // default to decoding uncompressed data - needed for replication, which enforces that
  // uncompressed edits are sent across the wire. In the regular case (reading/writing WAL), the
  // codec will be setup by the reader/writer class, not here.
  private WALEditCodec codec = new WALEditCodec();

  public WALEdit() {
  }

  /**
   * {@link #setCodec(WALEditCodec)} must be called before calling this method.
   * @param compression the {@link CompressionContext} for the underlying codec.
   */
  @SuppressWarnings("javadoc")
  public void setCompressionContext(final CompressionContext compression) {
    this.codec.setCompression(compression);
  }

  public void setCodec(WALEditCodec codec) {
    this.codec = codec;
  }


  public void add(KeyValue kv) {
    this.kvs.add(kv);
  }

  public boolean isEmpty() {
    return kvs.isEmpty();
  }

  public int size() {
    return kvs.size();
  }

  public List<KeyValue> getKeyValues() {
    return kvs;
  }

  public Integer getFromScope(byte[] key) {
    return scopes.get(key);
  }

  /**
   * @return the underlying replication scope map
   * @deprecated use {@link #getFromScope(byte[])} instead
   */
  @Deprecated
  public NavigableMap<byte[], Integer> getScopes() {
    return scopes;
  }

  /**
   * @param scopes set all the replication scope information. Must be non-<tt>null</tt>
   * @deprecated use {@link #putIntoScope(byte[], Integer)} instead. This completely overrides any
   *             existing scopes
   */
  @Deprecated
  public void setScopes(NavigableMap<byte[], Integer> scopes) {
    this.scopes.clear();
    this.scopes.putAll(scopes);
  }

  public void putIntoScope(byte[] key, Integer value) {
    scopes.put(key, value);
  }

  public boolean hasKeyInScope(byte[] key) {
    return scopes.containsKey(key);
  }

  /**
   * @return true if the cluster with the given clusterId has consumed the change.
   */
  public boolean hasClusterId(UUID clusterId) {
    return hasKeyInScope(Bytes.toBytes(PREFIX_CLUSTER_KEY + clusterId.toString()));
  }

  /**
   * Marks that the cluster with the given clusterId has consumed the change.
   */
  public void addClusterId(UUID clusterId) {
    scopes.put(Bytes.toBytes(PREFIX_CLUSTER_KEY + clusterId.toString()), 1);
  }

  /**
   * Marks that the clusters with the given clusterIds have consumed the change.
   */
  public void addClusterIds(List<UUID> clusterIds) {
    for (UUID clusterId : clusterIds) {
      addClusterId(clusterId);
    }
  }

  /**
   * @return the set of cluster Ids that have consumed the change.
   */
  public List<UUID> getClusterIds() {
    List<UUID> clusterIds = new ArrayList<UUID>();
    for (byte[] keyBytes : scopes.keySet()) {
      String key = Bytes.toString(keyBytes);
      if (key.startsWith(PREFIX_CLUSTER_KEY)) {
        clusterIds.add(UUID.fromString(key.substring(PREFIX_CLUSTER_KEY.length())));
      }
    }
    return clusterIds;
  }

  public void readFields(DataInput in) throws IOException {
    kvs.clear();
    scopes.clear();
    Decoder decoder = this.codec.getDecoder((DataInputStream) in);
    int versionOrLength = in.readInt();
    int length = versionOrLength;

    // make sure we get the real length
    if (versionOrLength == VERSION_2) {
      length = in.readInt();
    }

    // read in all the key values
    kvs.ensureCapacity(length);
    for(int i=0; i< length && decoder.advance(); i++) {
      kvs.add(decoder.current());
    }

    //its a new style WAL, so we need replication scopes too
    if (versionOrLength == VERSION_2) {
      int numEntries = in.readInt();
      if (numEntries > 0) {
        for (int i = 0; i < numEntries; i++) {
          byte[] key = Bytes.readByteArray(in);
          int scope = in.readInt();
          scopes.put(key, scope);
        }
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    Encoder kvEncoder = codec.getEncoder((DataOutputStream) out);
    out.writeInt(VERSION_2);

    //write out the keyvalues
    out.writeInt(kvs.size());
    for(KeyValue kv: kvs){
      kvEncoder.write(kv);
    }
    kvEncoder.flush();

    out.writeInt(scopes.size());
    for (byte[] key : scopes.keySet()) {
      Bytes.writeByteArray(out, key);
      out.writeInt(scopes.get(key));
    }
  }

  public long heapSize() {
    long ret = ClassSize.ARRAYLIST;
    for (KeyValue kv : kvs) {
      ret += kv.heapSize();
    }
    ret += ClassSize.TREEMAP;
    ret += ClassSize.align(scopes.size() * ClassSize.MAP_ENTRY);
    // TODO this isn't quite right, need help here
    return ret;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("[#edits: " + kvs.size() + " = <");
    for (KeyValue kv : kvs) {
      sb.append(kv.toString());
      sb.append("; ");
    }
    sb.append(" scopes: " + scopes.toString());
    sb.append(">]");
    return sb.toString();
  }
}
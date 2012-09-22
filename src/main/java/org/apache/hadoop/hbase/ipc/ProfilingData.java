package org.apache.hadoop.hbase.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.io.Writable;

/**
 * A map containing profiling data, mapping String to 
 * String, Long, Int, Boolean, and Float. This class is
 * not thread-safe.
 */

public class ProfilingData implements Writable {
  
  /**
   *  total amount of time spent server-side by the RPC
   */
  public static final String TOTAL_SERVER_TIME_MS = "total_server_time.ms";
  
  /**
   *  client reported network latency
   */
  public static final String CLIENT_NETWORK_LATENCY_MS = "client_network_latency.ms";
  
  /**
   *  number of block hits cnt on get
   */
  public static String blockHitCntStr(BlockType.BlockCategory cat, String cf) {
    return "block_hit_cnt." + cat + "." + cf;
  }
  
  /**
   *  number of block misses cnt on get
   */
  public static String blockMissCntStr(BlockType.BlockCategory cat, String cf) {
    return "block_miss_cnt." + cat + "." + cf;
  }
  
  /**
   *  on-disk size for the block misses
   */
  public static String blockMissOnDiskSizeStr(BlockType.BlockCategory cat, String cf) {
    return "block_miss_on_disk_size." + cat + "." + cf;
  }
  
  /**
   *  total time spent reading data blocks into cache on misses
   */
  public static final String TOTAL_BLOCK_READ_TIME_NS = "total_block_read_time.ns";
  
  /**
   *  time spend writing to HLog
   */
  public static final String HLOG_WRITE_TIME_MS = "hlog_write_time.ms";
  
  /**
   *  time spent syncing HLog
   */
  public static final String HLOG_SYNC_TIME_MS = "hlog_sync_time.ms";
  
  /**
   *  name of the rpc method called
   */
  public static final String RPC_METHOD_NAME = "rpc_method_name";
  
  /**
   *  separator used when concatenating strings to be merged
   */
  public static final String STRING_MERGE_SEPARATOR = ",";

	private Map<String, String> mapString = new HashMap<String, String>();
	private Map<String, MutableLong> mapLong = new HashMap<String, MutableLong>();
	private Map<String, MutableInt> mapInt = new HashMap<String, MutableInt>();
	private Map<String, Boolean> mapBoolean = new HashMap<String, Boolean>();
	private Map<String, MutableFloat> mapFloat = new HashMap<String, MutableFloat>();

	public ProfilingData() {}

	public void addString(String key, String val) {
		mapString.put(key, val);
	}

	public String getString(String key) {
	  return mapString.get(key);
	}
	
	public void addLong(String key, long val) {
    mapLong.put(key, new MutableLong(val));
  }

  public Long getLong(String key) {
    MutableLong ret = mapLong.get(key);
    if (ret == null) {
      return null;
    }
    return ret.toLong();
  }
  
  public void incLong(String key, long amt) {
    MutableLong dat = mapLong.get(key);
    if (dat == null) {
      this.addLong(key, amt);
    } else {
      dat.add(amt);
    }
  }

  public void incLong(String key) {
    this.incLong(key, 1);
  }

  public void decLong(String key, long amt) {
    this.incLong(key, -amt);
  }

  public void decLong(String key) {
    this.incLong(key, -1);
  }
  
  public void addInt(String key, int val) {
    mapInt.put(key, new MutableInt(val));
  }

  public Integer getInt(String key) {
    MutableInt ret = mapInt.get(key);
    if (ret == null) {
      return null;
    }
    return ret.toInteger();
  }

  public void incInt(String key, int amt) {
    MutableInt dat = mapInt.get(key);
    if (dat == null) {
      this.addInt(key, amt);
    } else {
      dat.add(amt);
    }
  }

  public void incInt(String key) {
    this.incInt (key, 1);
  }

  public void decInt(String key, int amt) {
    this.incInt(key, -amt);
  }

  public void decInt(String key) {
    this.decInt(key, 1);
  }
  
  public void addBoolean(String key, boolean val) {
    mapBoolean.put(key, val);
  }

  public Boolean getBoolean(String key) {
    return mapBoolean.get(key);
  }
  
  public void addFloat(String key, float val) {
    mapFloat.put(key, new MutableFloat (val));
  }

  public Float getFloat(String key) {
    MutableFloat ret = mapFloat.get(key);
    if (ret == null) {
      return null;
    }
    return ret.toFloat();
  }
  
  public void incFloat(String key, float amt) {
    MutableFloat dat = mapFloat.get(key);
    if (dat == null) {
      this.addFloat(key, amt);
    } else {
      dat.add(amt);
    }
  }
  
  public void decFloat(String key, float amt) {
    this.incFloat(key, -amt);
  }
  
  public void merge(ProfilingData pData) {
    // TODO: this just adds/concatenates data, maybe will need
    // to handle other merge types, eg max, min, average
    for (Map.Entry<String, String> entry : pData.mapString.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();
      String base = this.mapString.get(key);
      if (base == null) {
        this.mapString.put(key, val);
      } else {
        this.mapString.put(key, base + 
            ProfilingData.STRING_MERGE_SEPARATOR + val);
      }
    }
    for (Map.Entry<String, Boolean> entry : pData.mapBoolean.entrySet()) {
      String key = entry.getKey();
      boolean val = entry.getValue();
      Boolean base = this.mapBoolean.get(key);
      if (base == null) {
        this.mapBoolean.put(key, val);
      } else {
        this.mapBoolean.put(key, base || val);
      }
    }
    for (Map.Entry<String, MutableInt> entry : pData.mapInt.entrySet()) {
      String key = entry.getKey();
      MutableInt val = entry.getValue();
      MutableInt base = this.mapInt.get(key);
      if (base == null) {
        this.mapInt.put(key, new MutableInt(val.intValue()));
      } else {
        base.add(val);
      }
    }
    for (Map.Entry<String, MutableLong> entry : pData.mapLong.entrySet()) {
      String key = entry.getKey();
      MutableLong val = entry.getValue();
      MutableLong base = this.mapLong.get(key);
      if (base == null) {
        this.mapLong.put(key, new MutableLong(val.longValue()));
      } else {
        base.add(val);
      }
    }
    for (Map.Entry<String, MutableFloat> entry : pData.mapFloat.entrySet()) {
      String key = entry.getKey();
      MutableFloat val = entry.getValue();
      MutableFloat base = this.mapFloat.get(key);
      if (base == null) {
        this.mapFloat.put(key, new MutableFloat(val.floatValue()));
      } else {
        base.add(val);
      }
    }
  }
	
	@Override
	public void write(DataOutput out) throws IOException {
	  out.writeInt(mapString.size());
	  for (Map.Entry<String,String> entry : mapString.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
	  out.writeInt(mapBoolean.size());
    for (Map.Entry<String,Boolean> entry : mapBoolean.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeBoolean(entry.getValue());
    }
    out.writeInt(mapInt.size());
    for (Map.Entry<String,MutableInt> entry : mapInt.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeInt(entry.getValue().intValue());
    }
    out.writeInt(mapLong.size());
    for (Map.Entry<String,MutableLong> entry : mapLong.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeLong(entry.getValue().longValue());
    }
    out.writeInt(mapFloat.size());
    for (Map.Entry<String,MutableFloat> entry : mapFloat.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeFloat(entry.getValue().floatValue());
    }
	}
	  
	@Override
	public void readFields(DataInput in) throws IOException {
	  int size;
	  String key;
	  size = in.readInt();
	  mapString.clear();
    for (int i = 0; i < size; i ++) {
      key = in.readUTF();
      this.addString(key, in.readUTF());
    }
    size = in.readInt();
    mapBoolean.clear();
    for (int i = 0; i < size; i ++) {
      key = in.readUTF();
      this.addBoolean(key, in.readBoolean());
    }
    size = in.readInt();
    mapInt.clear();
    for (int i = 0; i < size; i ++) {
      key = in.readUTF();
      this.addInt(key, in.readInt());
    }
    size = in.readInt();
    mapLong.clear();
    for (int i = 0; i < size; i ++) {
      key = in.readUTF();
      this.addLong(key, in.readLong());
    }
    size = in.readInt();
    mapFloat.clear();
    for (int i = 0; i < size; i ++) {
      key = in.readUTF();
      this.addFloat(key, in.readFloat());
    }
	}
	
	public String toString(String delim) {
	  StringBuilder sb = new StringBuilder ();
    for (Map.Entry<String, String> entry : mapString.entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + delim);
    }
    for (Map.Entry<String, Boolean> entry : mapBoolean.entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + delim);
    }
    for (Map.Entry<String, MutableInt> entry : mapInt.entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + delim);
    }
    for (Map.Entry<String, MutableLong> entry : mapLong.entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + delim);
    }
    for (Map.Entry<String, MutableFloat> entry : mapFloat.entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + delim);
    }
    if (sb.length() >= delim.length()) {
      sb.delete(sb.length() - delim.length(), sb.length());
    }
    return sb.toString();
	}
	
	@Override
	public String toString() {
	  return this.toString(", ");
	}
	
	public String toPrettyString() {
	  return this.toString("\n");
  }
}

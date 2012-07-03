package org.apache.hadoop.hbase.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.WritableWithSize;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;

/*
 * A map containing profiling data
 * only maps String->String to be pretty printable
 */

public class ProfilingData implements Writable {

	private MapWritable mapString = new MapWritable ();
	private MapWritable mapLong = new MapWritable ();
	private MapWritable mapInt = new MapWritable ();
	private MapWritable mapBoolean = new MapWritable ();
	private MapWritable mapFloat = new MapWritable ();

	public ProfilingData () {}

	public void addString (String key, String val) {
		mapString.put (new BytesWritable (key.getBytes ()), 
		    new BytesWritable (val.getBytes ()));
	}

	public String getString (String key) {
		return new String (((BytesWritable) mapString.get 
		    (new BytesWritable (key.getBytes ()))).get ());
	}
	
	public void addLong (String key, long val) {
    mapLong.put (new BytesWritable (key.getBytes ()), 
        new LongWritable (val));
  }

  public long getLong (String key) {
    return ((LongWritable) mapLong.get 
        (new BytesWritable (key.getBytes ()))).get ();
  }
  
  public void addInt (String key, int val) {
    mapInt.put (new BytesWritable (key.getBytes ()), 
        new IntWritable (val));
  }

  public int getInt (String key) {
    return ((IntWritable) mapInt.get 
        (new BytesWritable (key.getBytes ()))).get ();
  }
  
  public void addBoolean (String key, boolean val) {
    mapBoolean.put (new BytesWritable (key.getBytes ()), 
        new BooleanWritable (val));
  }

  public boolean getBoolean (String key) {
    return ((BooleanWritable) mapBoolean.get 
        (new BytesWritable (key.getBytes ()))).get ();
  }
  
  public void addFloat (String key, float val) {
    mapFloat.put (new BytesWritable (key.getBytes ()), 
        new FloatWritable (val));
  }

  public float getFloat (String key) {
    return ((FloatWritable) mapFloat.get 
        (new BytesWritable (key.getBytes ()))).get ();
  }
	
	@Override
	public void write(DataOutput out) throws IOException {
	 	mapString.write (out);
	 	mapBoolean.write (out);
	 	mapInt.write (out);
	 	mapLong.write (out);
	 	mapFloat.write (out);
	}
	  
	@Override
	public void readFields(DataInput in) throws IOException {
	  mapString.readFields (in);
	  mapBoolean.readFields (in);
	  mapInt.readFields (in);
	  mapLong.readFields (in);
	  mapFloat.readFields (in);
	}
	
	@Override
	public String toString () {
	  StringBuilder sb = new StringBuilder ();
	  for (Map.Entry<Writable,Writable> entry : mapString.entrySet ()) {
	    sb.append (new String (((BytesWritable) entry.getKey ()).get ()) + " : " 
	        + new String (((BytesWritable) entry.getValue ()).get ()) + "\n");
	  }
	  for (Map.Entry<Writable,Writable> entry : mapBoolean.entrySet ()) {
      sb.append (new String (((BytesWritable) entry.getKey ()).get ()) + " : " 
          + ((BooleanWritable) entry.getValue ()).get () + "\n");
    }
	  for (Map.Entry<Writable,Writable> entry : mapInt.entrySet ()) {
      sb.append (new String (((BytesWritable) entry.getKey ()).get ()) + " : " 
          + ((IntWritable) entry.getValue ()).get () + "\n");
    }
	  for (Map.Entry<Writable,Writable> entry : mapLong.entrySet ()) {
      sb.append (new String (((BytesWritable) entry.getKey ()).get ()) + " : " 
          + ((LongWritable) entry.getValue ()).get () + "\n");
    }
	  for (Map.Entry<Writable,Writable> entry : mapFloat.entrySet ()) {
      sb.append (new String (((BytesWritable) entry.getKey ()).get ()) + " : " 
          + ((FloatWritable) entry.getValue ()).get () + "\n");
    }
	  return sb.toString ();
	}
}

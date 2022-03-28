/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonWriter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for doing JSON and MBeans.
 */
@InterfaceAudience.Private
public class JSONBean {
  private static final String COMMA = ",";
  private static final String ASTERICK = "*";
  private static final Logger LOG = LoggerFactory.getLogger(JSONBean.class);
  private static final Gson GSON = GsonUtil.createGson().create();

  /**
   * Use dumping out mbeans as JSON.
   */
  public interface Writer extends Closeable {

    void write(String key, String value) throws IOException;

    default int write(MBeanServer mBeanServer, ObjectName qry, String attribute,
        boolean description) throws IOException {
      return write(mBeanServer, qry, attribute, description, null);
    }

    int write(MBeanServer mBeanServer, ObjectName qry, String attribute, boolean description,
        ObjectName excluded) throws IOException;

    void flush() throws IOException;
  }

  /**
   * Notice that, closing the return {@link Writer} will not close the {@code writer} passed in, you
   * still need to close the {@code writer} by yourself.
   * <p/>
   * This is because that, we can only finish the json after you call {@link Writer#close()}. So if
   * we just close the {@code writer}, you can write nothing after finished the json.
   */
  public Writer open(final PrintWriter writer) throws IOException {
    JsonWriter jsonWriter = GSON.newJsonWriter(new java.io.Writer() {

      @Override
      public void write(char[] cbuf, int off, int len) throws IOException {
        writer.write(cbuf, off, len);
      }

      @Override
      public void flush() throws IOException {
        writer.flush();
      }

      @Override
      public void close() throws IOException {
        // do nothing
      }
    });
    jsonWriter.setIndent("  ");
    jsonWriter.beginObject();
    return new Writer() {
      @Override
      public void flush() throws IOException {
        jsonWriter.flush();
      }

      @Override
      public void close() throws IOException {
        jsonWriter.endObject();
        jsonWriter.close();
      }

      @Override
      public void write(String key, String value) throws IOException {
        jsonWriter.name(key).value(value);
      }

      @Override
      public int write(MBeanServer mBeanServer, ObjectName qry, String attribute,
          boolean description, ObjectName excluded) throws IOException {
        return JSONBean.write(jsonWriter, mBeanServer, qry, attribute, description, excluded);
      }
    };
  }

  /**
   * @return Return non-zero if failed to find bean. 0
   */
  private static int write(JsonWriter writer, MBeanServer mBeanServer, ObjectName qry,
      String attribute, boolean description, ObjectName excluded) throws IOException {
    LOG.debug("Listing beans for {}", qry);
    Set<ObjectName> names = null;
    names = mBeanServer.queryNames(qry, null);
    writer.name("beans").beginArray();
    Iterator<ObjectName> it = names.iterator();
    Pattern[] matchingPattern = null;
    while (it.hasNext()) {
      ObjectName oname = it.next();
      if (excluded != null && excluded.apply(oname)) {
        continue;
      }
      MBeanInfo minfo;
      String code = "";
      String descriptionStr = null;
      Object attributeinfo = null;
      try {
        minfo = mBeanServer.getMBeanInfo(oname);
        code = minfo.getClassName();
        if (description) {
          descriptionStr = minfo.getDescription();
        }
        String prs = "";
        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            prs = "modelerType";
            code = (String) mBeanServer.getAttribute(oname, prs);
          }
          if (attribute != null) {
            String[] patternAttr = null;
            if (attribute.contains(ASTERICK)) {
              if (attribute.contains(COMMA)) {
                patternAttr = attribute.split(COMMA);
              } else {
                patternAttr = new String[1];
                patternAttr[0] = attribute;
              }
              matchingPattern = new Pattern[patternAttr.length];
              for (int i = 0; i < patternAttr.length; i++) {
                matchingPattern[i] = Pattern.compile(patternAttr[i]);
              }
              // nullify the attribute
              attribute = null;
            } else {
              prs = attribute;
              attributeinfo = mBeanServer.getAttribute(oname, prs);
            }
          }
        } catch (RuntimeMBeanException e) {
          // UnsupportedOperationExceptions happen in the normal course of business,
          // so no need to log them as errors all the time.
          if (e.getCause() instanceof UnsupportedOperationException) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Getting attribute " + prs + " of " + oname + " threw " + e);
            }
          } else {
            LOG.error("Getting attribute " + prs + " of " + oname + " threw an exception", e);
          }
          return 0;
        } catch (AttributeNotFoundException e) {
          // If the modelerType attribute was not found, the class name is used
          // instead.
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
        } catch (MBeanException e) {
          // The code inside the attribute getter threw an exception so log it,
          // and fall back on the class name
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
        } catch (RuntimeException e) {
          // For some reason even with an MBeanException available to them
          // Runtime exceptionscan still find their way through, so treat them
          // the same as MBeanException
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
        } catch (ReflectionException e) {
          // This happens when the code inside the JMX bean (setter?? from the
          // java docs) threw an exception, so log it and fall back on the
          // class name
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", e);
        }
      } catch (InstanceNotFoundException e) {
        // Ignored for some reason the bean was not found so don't output it
        continue;
      } catch (IntrospectionException e) {
        // This is an internal error, something odd happened with reflection so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + oname, e);
        continue;
      } catch (ReflectionException e) {
        // This happens when the code inside the JMX bean threw an exception, so
        // log it and don't output the bean.
        LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + oname, e);
        continue;
      }
      writer.beginObject();
      writer.name("name").value(oname.toString());
      if (description && descriptionStr != null && descriptionStr.length() > 0) {
        writer.name("description").value(descriptionStr);
      }
      writer.name("modelerType").value(code);
      if (attribute != null && attributeinfo == null) {
        writer.name("result").value("ERROR");
        writer.name("message").value("No attribute with name " + attribute + " was found.");
        writer.endObject();
        writer.endArray();
        writer.close();
        return -1;
      }

      if (attribute != null) {
        writeAttribute(writer, attribute, descriptionStr, attributeinfo);
      } else {
        MBeanAttributeInfo[] attrs = minfo.getAttributes();
        for (int i = 0; i < attrs.length; i++) {
          writeAttribute(writer, mBeanServer, oname, description, matchingPattern, attrs[i]);
        }
      }
      writer.endObject();
    }
    writer.endArray();
    return 0;
  }

  private static void writeAttribute(JsonWriter writer, MBeanServer mBeanServer, ObjectName oname,
      boolean description, Pattern pattern[], MBeanAttributeInfo attr) throws IOException {
    if (!attr.isReadable()) {
      return;
    }
    String attName = attr.getName();
    if ("modelerType".equals(attName)) {
      return;
    }
    if (attName.indexOf("=") >= 0 || attName.indexOf(" ") >= 0) {
      return;
    }

    if (pattern != null) {
      boolean matchFound = false;
      for (Pattern compile : pattern) {
        // check if we have any match
        if (compile.matcher(attName).find()) {
          matchFound = true;
          break;
        }
      }
      if (!matchFound) {
        return;
      }
    }

    String descriptionStr = description ? attr.getDescription() : null;
    Object value = null;
    try {
      value = mBeanServer.getAttribute(oname, attName);
    } catch (RuntimeMBeanException e) {
      // UnsupportedOperationExceptions happen in the normal course of business,
      // so no need to log them as errors all the time.
      if (e.getCause() instanceof UnsupportedOperationException) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Getting attribute " + attName + " of " + oname + " threw " + e);
        }
      } else {
        LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
      }
      return;
    } catch (RuntimeErrorException e) {
      // RuntimeErrorException happens when an unexpected failure occurs in getAttribute
      // for example https://issues.apache.org/jira/browse/DAEMON-120
      LOG.debug("getting attribute " + attName + " of " + oname + " threw an exception", e);
      return;
    } catch (AttributeNotFoundException e) {
      // Ignored the attribute was not found, which should never happen because the bean
      // just told us that it has this attribute, but if this happens just don't output
      // the attribute.
      return;
    } catch (MBeanException e) {
      // The code inside the attribute getter threw an exception so log it, and
      // skip outputting the attribute
      LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
      return;
    } catch (RuntimeException e) {
      // For some reason even with an MBeanException available to them Runtime exceptions
      // can still find their way through, so treat them the same as MBeanException
      LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
      return;
    } catch (ReflectionException e) {
      // This happens when the code inside the JMX bean (setter?? from the java docs)
      // threw an exception, so log it and skip outputting the attribute
      LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", e);
      return;
    } catch (InstanceNotFoundException e) {
      // Ignored the mbean itself was not found, which should never happen because we
      // just accessed it (perhaps something unregistered in-between) but if this
      // happens just don't output the attribute.
      return;
    }

    writeAttribute(writer, attName, descriptionStr, value);
  }

  private static void writeAttribute(JsonWriter writer, String attName, String descriptionStr,
      Object value) throws IOException {
    if (descriptionStr != null && descriptionStr.length() > 0 && !attName.equals(descriptionStr)) {
      writer.name(attName);
      writer.beginObject();
      writer.name("description").value(descriptionStr);
      writer.name("value");
      writeObject(writer, value);
      writer.endObject();
    } else {
      writer.name(attName);
      writeObject(writer, value);
    }
  }

  private static void writeObject(JsonWriter writer, Object value) throws IOException {
    if (value == null) {
      writer.nullValue();
    } else {
      Class<?> c = value.getClass();
      if (c.isArray()) {
        writer.beginArray();
        int len = Array.getLength(value);
        for (int j = 0; j < len; j++) {
          Object item = Array.get(value, j);
          writeObject(writer, item);
        }
        writer.endArray();
      } else if (value instanceof Number) {
        Number n = (Number) value;
        if (Double.isFinite(n.doubleValue())) {
          writer.value(n);
        } else {
          writer.value(n.toString());
        }
      } else if (value instanceof Boolean) {
        Boolean b = (Boolean) value;
        writer.value(b);
      } else if (value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        writer.beginObject();
        for (String key : keys) {
          writeAttribute(writer, key, null, cds.get(key));
        }
        writer.endObject();
      } else if (value instanceof TabularData) {
        TabularData tds = (TabularData) value;
        writer.beginArray();
        for (Object entry : tds.values()) {
          writeObject(writer, entry);
        }
        writer.endArray();
      } else {
        writer.value(value.toString());
      }
    }
  }

  /**
   * Dump out all registered mbeans as json on System.out.
   */
  public static void dumpAllBeans() throws IOException, MalformedObjectNameException {
    try (PrintWriter writer =
      new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
      JSONBean dumper = new JSONBean();
      try (JSONBean.Writer jsonBeanWriter = dumper.open(writer)) {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        jsonBeanWriter.write(mbeanServer, new ObjectName("*:*"), null, false);
      }
    }
  }
}

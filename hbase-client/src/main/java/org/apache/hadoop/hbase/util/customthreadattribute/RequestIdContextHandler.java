package org.apache.hadoop.hbase.util.customthreadattribute;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.slf4j.MDC;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RequestIdContextHandler implements AttributeTypeHandler {

  private static String KEY = "RequestID";

  RequestIdContextHandler() {
  }

  @Override public List<CustomThreadAttribute> getAllAttributes() {
    List<CustomThreadAttribute> list = new ArrayList<>();
    CustomThreadAttribute attribute = getAttribute(null);
    if (attribute != null) {
      list.add(attribute);
    }
    return list;
  }

  @Override public void setAttribute(String key, Object value) {
    MDC.put(KEY, value.toString());
  }

  @Override public void clearAttribute(String key) {
    MDC.clear();
  }

  @Override public CustomThreadAttribute getAttribute(String key) {
    return new CustomThreadAttribute(KEY, MDC.get(KEY), null);
  }
}

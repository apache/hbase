package cn.enncloud.metric;

import org.avaje.metric.*;
import org.avaje.metric.report.ReportMetrics;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/** Created by jessejia on 16/8/26. */
public class OpentsdbJsonVisitor implements MetricVisitor {
  static final String KEY_JSON_METRIC = "metric";
  static final String KEY_JSON_TIMESTAMP = "timestamp";
  static final String KEY_JSON_VALUE = "value";
  static final String KEY_JSON_TAGS = "tags";
  static final String KEY_JSON_TAG_COUNT = "count";
  static final String KEY_JSON_TAG_MAX = "max";
  static final String KEY_JSON_TAG_TOTAL = "total";
  static final String KEY_JSON_TAG_KEY = "key";
  static final String KEY_JSON_TAG_HOST = "host";

  static final String METRIC_NAME_DELIMITER = ".";
  static final String TIME_METRIC_SUCCESS_SUFFIX = "success";
  static final String TIME_METRIC_ERROR_SUFFIX = "error";

  private JSONArray jsonArray;
  private ReportMetrics reportMetrics;
  private long collectionTime;

  /**
   * The constructor for OpentsdbJsonVisitor.
   *
   * @param reportMetrics metric for reporting.
   */
  public OpentsdbJsonVisitor(ReportMetrics reportMetrics) {
    this.jsonArray = new JSONArray();
    this.reportMetrics = reportMetrics;
    this.collectionTime = reportMetrics.getCollectionTime();
  }

  /**
   * Return a JSONArray for reporting to OpenTSDB.
   *
   * @return a JSONArray
   * @throws IOException while visiting metric
   */
  public JSONArray getJsonArray() throws IOException {
    if (this.reportMetrics == null || this.reportMetrics.getMetrics() == null) {
      return jsonArray;
    }

    List<Metric> metrics = this.reportMetrics.getMetrics();
    for (Metric metric : metrics) {
      metric.visit(this);
    }
    return jsonArray;
  }

  @Override
  public void visit(TimedMetric timedMetric) {
    String metricName = timedMetric.getName().toString();
    JSONObject successJsonObject =
        getJsonObject(
            timedMetric.getCollectedSuccessStatistics(),
            metricName + METRIC_NAME_DELIMITER + TIME_METRIC_SUCCESS_SUFFIX);
    JSONObject errorJsonObject =
        getJsonObject(
            timedMetric.getCollectedErrorStatistics(),
            metricName + METRIC_NAME_DELIMITER + TIME_METRIC_ERROR_SUFFIX);
    addJsonObject(successJsonObject);
    addJsonObject(errorJsonObject);
  }

  @Override
  public void visit(BucketTimedMetric bucketTimedMetric) throws IOException {
    for (TimedMetric timedMetric : bucketTimedMetric.getBuckets()) {
      visit(timedMetric);
    }
  }

  @Override
  public void visit(ValueMetric valueMetric) throws IOException {
    JSONObject jsonObject =
        getJsonObject(valueMetric.getCollectedStatistics(), valueMetric.getName().toString());
    addJsonObject(jsonObject);
  }

  @Override
  public void visit(CounterMetric counterMetric) throws IOException {
    JSONObject jsonObject =
        getJsonObject(counterMetric.getCollectedStatistics(), counterMetric.getName().toString());
    addJsonObject(jsonObject);
  }

  @Override
  public void visit(GaugeDoubleMetric gaugeDoubleMetric) throws IOException {
    JSONObject jsonObject =
        getJsonObject(gaugeDoubleMetric.getValue(), gaugeDoubleMetric.getName().toString());
    addJsonObject(jsonObject);
  }

  @Override
  public void visit(GaugeDoubleGroup gaugeDoubleGroup) throws IOException {
    for (GaugeDoubleMetric gaugeDoubleMetric : gaugeDoubleGroup.getGaugeMetrics()) {
      visit(gaugeDoubleMetric);
    }
  }

  @Override
  public void visit(GaugeLongMetric gaugeLongMetric) throws IOException {
    JSONObject jsonObject =
        getJsonObject(gaugeLongMetric.getValue(), gaugeLongMetric.getName().toString());
    addJsonObject(jsonObject);
  }

  @Override
  public void visit(GaugeLongGroup gaugeLongGroup) throws IOException {
    for (GaugeLongMetric gaugeLongMetric : gaugeLongGroup.getGaugeMetrics()) {
      visit(gaugeLongMetric);
    }
  }

  private JSONObject getJsonObject(double value, String metricName) {
    JSONObject jsonObject = getPartialJsonObject(metricName);
    jsonObject.put(KEY_JSON_VALUE, value);
    return jsonObject;
  }

  private JSONObject getJsonObject(long value, String metricName) {
    JSONObject jsonObject = getPartialJsonObject(metricName);
    jsonObject.put(KEY_JSON_VALUE, value);
    return jsonObject;
  }

  private JSONObject getJsonObject(CounterStatistics counterStatistics, String metricName) {
    if (counterStatistics == null) {
      return null;
    }
    JSONObject jsonObject = getPartialJsonObject(metricName);
    jsonObject.put(KEY_JSON_VALUE, counterStatistics.getCount());
    return jsonObject;
  }

  private JSONObject getJsonObject(ValueStatistics valueStatistics, String metricName) {
    if (valueStatistics == null) {
      return null;
    }
    JSONObject jsonObject = getPartialJsonObject(metricName);
    jsonObject.put(KEY_JSON_VALUE, valueStatistics.getMean());
    JSONObject tags = (JSONObject) jsonObject.get(KEY_JSON_TAGS);
    tags.put(KEY_JSON_TAG_COUNT, valueStatistics.getCount());
    tags.put(KEY_JSON_TAG_MAX, valueStatistics.getMax());
    tags.put(KEY_JSON_TAG_TOTAL, valueStatistics.getTotal());
    return jsonObject;
  }

  private JSONObject getPartialJsonObject(String metricName) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put(KEY_JSON_METRIC, metricName);
    jsonObject.put(KEY_JSON_TIMESTAMP, this.collectionTime);
    HashMap<String, Object> tags = new HashMap<>();
    tags.put(KEY_JSON_TAG_KEY, reportMetrics.getHeaderInfo().getKey());
    tags.put(KEY_JSON_TAG_HOST, reportMetrics.getHeaderInfo().getServer());
    jsonObject.put(KEY_JSON_TAGS, new JSONObject(tags));
    return jsonObject;
  }

  private void addJsonObject(JSONObject jsonObject) {
    if (jsonObject != null) {
      this.jsonArray.add(jsonObject);
    }
  }
}

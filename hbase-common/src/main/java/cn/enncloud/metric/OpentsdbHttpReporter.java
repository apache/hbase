package cn.enncloud.metric;

import cn.enncloud.metric.config.OpentsdbConfig;
import com.google.inject.Inject;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.avaje.metric.report.MetricReporter;
import org.avaje.metric.report.ReportMetrics;
import org.json.simple.JSONArray;

import java.io.IOException;

/** Http reporter for OpentTSDB. Created by jessejia on 16/8/30. */
public class OpentsdbHttpReporter implements MetricReporter {
  private HttpClient httpClient;
  private OpentsdbConfig config;

  @Inject
  private OpentsdbHttpReporter(OpentsdbConfig config, HttpClient client) {
    this.config = config;
    this.httpClient = client;
  }

  @Override
  public void report(ReportMetrics reportMetrics) {
    OpentsdbJsonVisitor visitor = new OpentsdbJsonVisitor(reportMetrics);
    JSONArray jsonArray = null;
    try {
      jsonArray = visitor.getJsonArray();
    } catch (IOException e) {
      // It will never happen when using OpentsdbJsonVisitor
    }
    HttpPost httpPost = createPostRequest(jsonArray);
    try {
      httpClient.execute(httpPost);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void cleanup() {}

  private HttpPost createPostRequest(JSONArray jsonArray) {
    if (jsonArray == null) {
      return null;
    }

    String url = buildUrl(config);
    HttpPost post = new HttpPost(url);
    post.setEntity(new StringEntity(jsonArray.toJSONString(), ContentType.APPLICATION_JSON));
    return post;
  }

  private String buildUrl(OpentsdbConfig config) {
    return "http://" + config.getHostname() + ":" + config.getPort() + "/api/put";
  }
}

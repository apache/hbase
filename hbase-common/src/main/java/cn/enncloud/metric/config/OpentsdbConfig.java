package cn.enncloud.metric.config;

import java.io.IOException;

/** Created by jessejia on 16/8/31. */
public class OpentsdbConfig {
  private String hostname;
  private int port;

  private OpentsdbConfig() {}

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private String hostname;
    private int port = -1;

    private Builder() {}

    /**
     * The only method to build OpentsdbConfig.
     *
     * @return OpentsdbConfig
     * @throws IOException while not giving required parameters
     */
    public OpentsdbConfig build() throws IOException {
      validateConfig();

      OpentsdbConfig config = new OpentsdbConfig();
      config.hostname = this.hostname;
      config.port = this.port;

      return config;
    }

    private void validateConfig() throws IOException {
      // For now, only check the parameter was set or not
      if (this.port == -1 || this.hostname == null) {
        throw new IOException("OpentsdbConfig need specify both hostname and port");
      }
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }
  }

  @Override
  public String toString() {
    return "OpentsdbConfig{" + "hostname='" + hostname + '\'' + ", port=" + port + '}';
  }
}

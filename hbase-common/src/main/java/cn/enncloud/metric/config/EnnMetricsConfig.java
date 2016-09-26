package cn.enncloud.metric.config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/** Created by jessejia on 16/9/2. */
public class EnnMetricsConfig {
  private String hostname;
  private String ipAddress;
  private int freqInSeconds;

  private EnnMetricsConfig() {}

  public String getHostname() {
    return hostname;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public int getFreqInSeconds() {
    return freqInSeconds;
  }

  @Override
  public String toString() {
    return "EnnMetricsConfig{"
        + "hostname='"
        + hostname
        + '\''
        + ", ipAddress='"
        + ipAddress
        + '\''
        + ", freqInSeconds="
        + freqInSeconds
        + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static EnnMetricsConfig getDefaultConfig() throws UnknownHostException, IOException {
    return getConfigWithFreq(1);
  }

  /**
   * Get EnnMetricsConfig with specified frequency.
   *
   * @param freqInSeconds frequency in seconds
   * @return EnnMetricsConfig with specified frequency
   * @throws UnknownHostException while can not get the local host
   * @throws IOException will not meet this exception in this method
   */
  public static EnnMetricsConfig getConfigWithFreq(int freqInSeconds)
      throws UnknownHostException, IOException {
    InetAddress localHost = InetAddress.getLocalHost();
    String hostname = localHost.getHostName();
    String ip = localHost.getHostAddress();
    return newBuilder()
        .setHostname(hostname)
        .setIpAddress(ip)
        .setFreqInSeconds(freqInSeconds)
        .build();
  }

  public static final class Builder {
    private String hostname;
    private String ipAddress;
    private int freqInSeconds = -1;

    private Builder() {}

    /**
     * The only method to build EnnMetricsConfig.
     *
     * @return EnnMetricsConfig
     * @throws IOException while not giving required parameters
     */
    public EnnMetricsConfig build() throws IOException {
      validateConfig();

      EnnMetricsConfig config = new EnnMetricsConfig();
      config.hostname = this.hostname;
      config.ipAddress = this.ipAddress;
      config.freqInSeconds = this.freqInSeconds;

      return config;
    }

    private void validateConfig() throws IOException {
      // For now, only check the parameter was set or not
      if (this.freqInSeconds == -1 || this.hostname == null || this.ipAddress == null) {
        throw new IOException("EnnMetricsConfig need theses parameters: hostname, ipAddress and freqInSeconds");
      }
    }

    public Builder setHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public Builder setIpAddress(String ipAddress) {
      this.ipAddress = ipAddress;
      return this;
    }

    public Builder setFreqInSeconds(int freqInSeconds) {
      this.freqInSeconds = freqInSeconds;
      return this;
    }
  }
}

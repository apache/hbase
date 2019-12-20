package org.apache.hadoop.hbase.security.provider.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;

public class ShadeTokenIdentifier extends TokenIdentifier {
  private String username;

  public ShadeTokenIdentifier(String username) {
    this.username = username;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(username);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    username = in.readUTF();
  }

  @Override
  public Text getKind() {
    return ShadeSaslAuthenticationProvider.TOKEN_KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    if (username == null || "".equals(username)) {
      return null;
    }
    return UserGroupInformation.createRemoteUser(username);
  }
}

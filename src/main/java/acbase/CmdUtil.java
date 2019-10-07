package acbase;

import java.io.InputStream;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;

public abstract class CmdUtil {
  public static AccumuloClient getConnector() throws Exception {
    
    
    Properties props = new Properties();
    try (InputStream propsFile = CmdUtil.class.getResourceAsStream("/accumulo-client.properties")) {
      if(propsFile == null) {
        throw new Exception("accumulo-client.properties not found on classpath");
      }
      props.load(propsFile);
    }

    return Accumulo.newClient().from(props).build();
  }
}

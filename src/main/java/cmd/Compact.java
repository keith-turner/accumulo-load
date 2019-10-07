package cmd;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;

import acbase.CmdUtil;

public class Compact {
  public static void main(String[] args) throws Exception {
    
    if (args.length != 2) {
      System.err.println(Ingest.class.getSimpleName() + " <table> <sleep>");
    }
    
    String table = args[0];
    int sleep = Integer.parseInt(args[1]);
    
    try (AccumuloClient client = CmdUtil.getConnector()) {
      while(true) {
        try {
          Thread.sleep(sleep);
          long t1 = System.currentTimeMillis();
          client.tableOperations().compact(table, new CompactionConfig().setWait(true).setFlush(true));
          long t2 = System.currentTimeMillis();
          System.out.println("COMPACT "+t2+" "+(t2 - t1));
        } catch (InterruptedException | AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}

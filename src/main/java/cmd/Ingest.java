package cmd;

import java.util.Random;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.FastFormat;

import com.google.common.util.concurrent.RateLimiter;

import acbase.CmdUtil;

public class Ingest {
  
  private static final byte[] FAM = "pinky".getBytes();
  private static final byte[] E = new byte[] {};
  
  static long getMask(int maskSize) {
    if(maskSize > 63) {
      throw new IllegalArgumentException(" maskSize "+maskSize);
    }
    
    long mask = 0;
    
    for(int i = 0; i< maskSize; i++) {
      mask = 1L<<62 | mask>>1;
    }
    
    return mask;
  }
  
  public static void main(String[] args) throws Exception {
    if(args.length != 4 && args.length != 5) {
      System.err.println(Ingest.class.getSimpleName()+" <table> <rows> <mask size> <cols> [rate limit]");
    }
    
    RateLimiter limiter = null;
    
    if(args.length == 5) {
      limiter = RateLimiter.create(Double.parseDouble(args[4]));
      
    }
    String table = args[0];
    int rows = Integer.parseInt(args[1]);
    int maskSize = Integer.parseInt(args[2]);
    int cols = Integer.parseInt(args[3]);
    
    try(AccumuloClient client = CmdUtil.getConnector()){
      write(client, table, rows, getMask(maskSize), cols, limiter);
    }
  }
  
  private static void write(AccumuloClient conn, String table, int rows, long mask, int cols, RateLimiter limiter) throws Exception {

    try(BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig())){
      Random rand = new Random();

      for (int r = 0; r < rows; r++) {
        byte[] row = FastFormat.toZeroPaddedString(mask & rand.nextLong(), 16, 16, E);
        Mutation m = new Mutation(row);
        
        for (int c = 0; c < cols; c++) {
          byte[] qual = FastFormat.toZeroPaddedString(c, 4, 16, E);

          byte[] val = new byte[32];
          rand.nextBytes(val);

          m.put(FAM, qual, val);
        }

        if(limiter != null) 
          limiter.acquire(cols);
        
        bw.addMutation(m);
      }

    }

  }
}

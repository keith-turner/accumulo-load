package cmd;

import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.FastFormat;

import com.google.common.util.concurrent.RateLimiter;

import acbase.CmdUtil;

public class Scan {

  private static final byte[] E = new byte[] {};

  public static void main(String[] args) throws Exception {
    if (args.length != 5) {
      System.err.println(Ingest.class.getSimpleName() + " <table> <lookups> <masksize> <rate limit> <threads>");
    }

    String table = args[0];
    AtomicInteger lookups = new AtomicInteger(Integer.parseInt(args[1]));
    int maskSize = Integer.parseInt(args[2]);
    int limit = Integer.parseInt(args[3]);
    int threads = Integer.parseInt(args[4]);

    RateLimiter limiter = limit > 0 ? RateLimiter.create(limit) : null;

    ExecutorService tp = Executors.newFixedThreadPool(threads);
    try (AccumuloClient client = CmdUtil.getConnector()) {
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(tp.submit(() -> scan(client, table, lookups, Ingest.getMask(maskSize), limiter)));
      }

      for (Future<?> future : futures) {
        future.get();
      }
    }
    
    tp.shutdown();

  }

  private static void scan(AccumuloClient client, String table, AtomicInteger lookups, long mask, RateLimiter limiter) {
    try {
      Scanner scanner = client.createScanner(table, Authorizations.EMPTY);

      Random rand = new Random();

      while (lookups.decrementAndGet() > 0) {

        byte[] row = FastFormat.toZeroPaddedString(mask & rand.nextLong(), 16, 16, E);

        scanner.setRange(Range.exact(new String(row)));

        if (limiter != null)
          limiter.acquire();

        long t1 = System.currentTimeMillis();
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {
          count++;
        }
        long t2 = System.currentTimeMillis();

        System.out.println("SCAN " + t2 + " " + (t2 - t1) + " " + count);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

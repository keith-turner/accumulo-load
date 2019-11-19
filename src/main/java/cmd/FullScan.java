package cmd;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.collect.Iterables;

import acbase.CmdUtil;

public class FullScan {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println(FullScan.class.getSimpleName() + " <table> <threads>");
    }

    String table = args[0];
    int threads = Integer.parseInt(args[1]);

    Random rand = new Random();

    ExecutorService es = Executors.newFixedThreadPool(threads);

    try (AccumuloClient client = CmdUtil.getConnector()) {
      for (int i = 0; i < threads; i++) {
        es.execute(() -> fullScan(table, client, rand));
      }

      es.shutdown();

      es.awaitTermination(1024, TimeUnit.DAYS);
    }
  }

  private static void fullScan(String table, AccumuloClient client, Random rand) {
    Scanner scanner;
    try {
      scanner = client.createScanner(table, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    scanner.setExecutionHints(Map.of("scan_type", "background"));

    while (true) {
      String startRow = String.format("%016x", Math.abs(rand.nextLong()));

      Range range1 = new Range(startRow, null);
      Range range2 = new Range(null, startRow);

      long t1 = System.currentTimeMillis();

      scanner.setRange(range1);
      int count = Iterables.size(scanner);
      scanner.setRange(range2);
      count += Iterables.size(scanner);

      long t2 = System.currentTimeMillis();

      System.out.println("FULLSCAN " + t2 + " " + (t2 - t1) + " " + count);
    }
  }
}

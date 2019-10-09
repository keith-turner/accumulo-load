package cmd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class Summarize {

  static class Summary {
    SummaryStatistics stats = new SummaryStatistics();
    int compaction;
  }

  public static void main(String[] args) throws IOException {

    TreeMap<Long,Summary> sm = new TreeMap<>();

    try (Scanner iter = new Scanner(System.in)) {

      while (iter.hasNextLine()) {
        processLine(sm, iter.nextLine());

      }

    }

    long minTime = sm.firstKey();

    sm.forEach((t, s) -> System.out.printf("%d %.2f %.2f %d %d\n", (t - minTime), s.stats.getMean(),
        s.stats.getStandardDeviation(), s.stats.getN(), s.compaction));

  }

  private static void processLine(TreeMap<Long,Summary> sm, String line) {
    try {
      var cols = line.split("\\s+");

      switch (cols[0]) {
        case "SCAN": {
          long time = Long.parseLong(cols[1]);
          long duration = Long.parseLong(cols[2]);

          sm.computeIfAbsent(time / 1000, k -> new Summary()).stats.addValue(duration);

          break;
        }
        case "COMPACT": {
          long endTime = Long.parseLong(cols[1]);
          long duration = Long.parseLong(cols[2]);
          long startTime = (endTime - duration) / 1000;
          endTime = endTime / 1000;

          for (long t = startTime; t <= endTime; t++) {
            sm.computeIfAbsent(t, k -> new Summary()).compaction++;
          }

          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("line : " + line, e);
    }
  }
}

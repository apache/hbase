package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.RuntimeExceptionAbortStrategy;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper.NodeFilter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class ZooKeeperWrapperMain {

  @SuppressWarnings("serial")
  private static class UsageException extends Exception {

    public UsageException() {
      super();
    }

    public UsageException(String message) {
      super(message);
    }

    public UsageException(Throwable cause) {
      super(cause);
    }

  }

  private static class TimeParser {

    static final SimpleDateFormat format = new SimpleDateFormat("yy/MM/dd HH:mm:ss");

    public static long parse(String time)
      throws UsageException {
      try {
        return format.parse(time).getTime();
      } catch (java.text.ParseException e) {
        // nothing to do
      }
      try {
        return Long.parseLong(time);
      } catch (NumberFormatException e) {
        // nothing to do
      }
      throw new UsageException("Failed to parse a time string");
    }

    public static void help() {
      Date now = new Date();
      System.err.println("timestamp format is either '" + format.format(now) + "' or '" + now.getTime() + "'");
    }
  }

  private static ZooKeeperWrapper createZooKeeperWrapper() {
    Configuration conf = HBaseConfiguration.create();
    try {
      return ZooKeeperWrapper.createInstance(conf, "zkwrap", new RuntimeExceptionAbortStrategy());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a ZooKeeperWrapper instance", e);
    }
  }

  static abstract class Command {

    private static Options opts = new Options();

    protected static Options getOptions() {
      return opts;
    }

    protected abstract void help();

    protected abstract void run(CommandLine line)
      throws UsageException;

    public void run(String[] args) {
      try {
        CommandLineParser parser = new BasicParser();
        CommandLine line;
        try {
          line = parser.parse(opts, args);
        } catch (ParseException e) {
          throw new UsageException(e);
        }
        run(line);
      } catch (UsageException e) {
        if (e.getMessage() != null) {
          System.err.println(e.getMessage());
        }
        else {
          help();
        }
      }
    }

  }

  static class DeleteCommand extends Command {

    static {
      getOptions().addOption("r", false, "delete children recursively (a required option)");
      getOptions().addOption("st", true, "starting timestamp");
      getOptions().addOption("en", true, "ending timestamp (not including)");
    }

    protected void help() {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(
          "delete [options] <path>",
          getOptions()
      );
      TimeParser.help();
    }

    protected void run(CommandLine line) throws UsageException {
      ZooKeeperWrapper zkWrapper = createZooKeeperWrapper();
      if (line.hasOption("r")) {
        if (line.getArgs().length != 1) {
          throw new UsageException("one path is expected");
        }
        String path = line.getArgs()[0];
        long st = TimeParser.parse(line.getOptionValue("st", "-1"));
        long en = TimeParser.parse(line.getOptionValue("en", "-1"));
        try {
          zkWrapper.deleteChildrenRecursively(path, new NodeFilter(st, en));
        }
        catch (KeeperException e) {
          throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      else {
        throw new UsageException();
      }
    }

  }

  private void help() {
    String message =
      "supported commands:\n" +
      "  delete\n";
    System.err.print(message);
  }

  private void run(String args[]) {
    if (args.length == 0) {
      help();
      return;
    }

    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
    Logger.getLogger("org.apache.hadoop.hbase.zookeeper").setLevel(Level.WARN);

    String commandName = args[0];
    String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);

    Map<String, Command> commands = new HashMap<String, Command>();
    commands.put("delete", new DeleteCommand());

    if (commands.containsKey(commandName)) {
      commands.get(commandName).run(commandArgs);
    }
    else {
      help();
    }
  }

  public static void main(String args[]) {
    (new ZooKeeperWrapperMain()).run(args);
  }

}

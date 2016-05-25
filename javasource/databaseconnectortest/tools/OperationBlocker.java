package databaseconnectortest.tools;

import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.mendix.logging.ILogNode;

public class OperationBlocker {
  private ILogNode logNode;
  private long sleepTime;
  private String name;
  private Supplier<Boolean> isBusy;

  public OperationBlocker(Supplier<Boolean> isReady, Long sleepTimeInSeconds, ILogNode logNode, String name) {
    this.logNode = logNode;
    this.sleepTime = sleepTimeInSeconds;
    this.name = name;
    this.isBusy = isReady;
  }

  public static void blockUntilReady(Supplier<Boolean> isBusy, Long sleepTimeInSeconds, ILogNode logNode, String name) {
    new OperationBlocker(isBusy, sleepTimeInSeconds, logNode, name).blockUntilReady();
  }

  public void blockUntilReady() {
    Stream.generate(isBusy)
      .peek(this::sleepIfBusy)
      .anyMatch(Predicate.isEqual(false));
    logNode.info("Operation " + name + " completed!");
  }

  private void sleepIfBusy(boolean isBusy) {
    if (isBusy) {
      logNode.info("Operation " + name + " is being performed at the moment. Please wait!");

      try {
        TimeUnit.SECONDS.sleep(sleepTime);
      } catch (InterruptedException e) { }
    }
  }
}

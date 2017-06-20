package databaseconnector.impl;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;

import com.zaxxer.hikari.HikariDataSource;
import databaseconnector.interfaces.ConnectionManager;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Simple connection manager backed by HikariCP. It does not support shutdown at this moment that may lead to memory leak in case of
 * hot deployment.
 * <p>
 * TODO: REMOVE TECHNICAL DEBT - extract connection manager into shared component, so that :
 * 1. the same connection manager may be used by actions, data storage layer (ConnectionBus), etc
 * 2. all connections may be properly closed during application shutdown
 */
public class JdbcConnectionManager implements ConnectionManager {
  private final Map<Integer, HikariDataSource> connectionPool = new ConcurrentHashMap<>();
  private final ILogNode logNode;
  private boolean hasDriversInitialized;

  public JdbcConnectionManager(final ILogNode logNode) {
    this.logNode = logNode;
  }

  public JdbcConnectionManager() {
    this(Core.getLogger(JdbcConnectionManager.class.getName()));
  }

  @Override
  public Connection getConnection(final String jdbcUrl, final String userName, final String password) throws SQLException {
    initializeDrivers();

    final Integer connPoolKey = toConnPoolKey(jdbcUrl, userName);
    final HikariDataSource dataSource = connectionPool.computeIfAbsent(connPoolKey, k -> {
      logNode.trace(String.format("Creating data source in connection pool for [url=%s, user=%s]", jdbcUrl, userName));
      return createHikariDataSource(jdbcUrl, userName, password, connPoolKey);
    });
    logNode.trace(String.format("Getting connection from data source in connection pool for [url=%s, user=%s]", jdbcUrl, userName));
    return dataSource.getConnection();
  }

  /**
   * The JDBC drivers in the userlib folder of a project are not automatically correctly registered to the DriverManager.
   * The cause is maybe the fact that the drivers are put into the project.jar on deployment.
   * Iterating through the drivers found by ServiceLoader.load(Driver.class) will force them to load and
   * register to the DriverManager.
   */
  private synchronized void initializeDrivers() {
    if (!hasDriversInitialized) {
      ServiceLoader<Driver> loader = ServiceLoader.load(Driver.class);
      Stream<String> driverNames = StreamSupport.stream(loader.spliterator(), false).map(a -> a.getClass().getName());
      String logMessage = driverNames.collect(Collectors.joining(", ", "Found JDBC Drivers: ", ""));
      logNode.trace(logMessage);
      hasDriversInitialized = true;
    }
  }

  private Integer toConnPoolKey(final String jdbcUrl, final String userName) {
    return (jdbcUrl + userName).hashCode();
  }

  private HikariDataSource createHikariDataSource(final String jdbcUrl, final String userName, final String password, Integer connPoolKey) {
    final HikariDataSource dataSource = new HikariDataSource();
    dataSource.setPoolName(String.format("MxDbConnector-HikaryCP-%d", connPoolKey));
    dataSource.setJdbcUrl(jdbcUrl);
    dataSource.setUsername(userName);
    dataSource.setPassword(password);
    dataSource.setMinimumIdle(0);

    return dataSource;
  }
}

package databaseconnector.impl;

import java.sql.Connection;
import java.sql.SQLException;

import databaseconnector.interfaces.ConnectionManager;
import databaseconnector.interfaces.IDatabaseAccessor;

public class DatabaseAccessorImpl implements IDatabaseAccessor {

  private final String jdbcUrl;
  private final String jdbcUsername;
  private final String jdbcPassword;
  private final ConnectionManager connectionManager;

  public DatabaseAccessorImpl(ConnectionManager connectionManager, String jdbcUrl, String jdbcUsername, String jdbcPassword) {
    this.jdbcUrl = jdbcUrl;
    this.jdbcUsername = jdbcUsername;
    this.jdbcPassword = jdbcPassword;
    this.connectionManager = connectionManager;
  }

  @Override
  public Connection getConnection() throws SQLException {
     return connectionManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword);
    }
}
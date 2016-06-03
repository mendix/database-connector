package databaseconnector.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.interfaces.ConnectionManager;
import databaseconnector.interfaces.ObjectInstantiator;

public class JdbcConnector {
  private final ILogNode logNode;
  private ObjectInstantiator objectInstantiator;
  private ConnectionManager connectionManager;

  public JdbcConnector(final ILogNode logNode, ObjectInstantiator objectInstantiator, ConnectionManager connectionManager) {
    this.logNode = logNode;
    this.objectInstantiator = objectInstantiator;
    this.connectionManager = connectionManager;
  }

  public JdbcConnector(ILogNode logNode) {
    this(logNode, new ObjectInstantiatorImpl(), ConnectionManagerSingleton.getInstance());
  }

  public Stream<IMendixObject> executeQuery(String jdbcUrl, String userName, String password, String entityName, String sql,
      IContext context) throws SQLException {
    Function<Map<String, Object>, IMendixObject> toMendixObject = columns -> {
      IMendixObject obj = objectInstantiator.instantiate(context, entityName);
      columns.forEach((n, v) -> obj.setValue(context, n, v));
      logNode.info("obj: " + obj);
      return obj;
    };

    Stream<Map<String,Object>> stream = executeQuery(jdbcUrl, userName, password, sql);

    return stream.map(toMendixObject);
  }

  public String executeQueryToJson(String jdbcUrl, String userName, String password, String sql, IContext context) throws SQLException {
    return null;
  }

  public Stream<Map<String, Object>> executeQuery(String jdbcUrl, String userName, String password, String sql) throws SQLException {
    logNode.info(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));
    Builder<Map<String, Object>> builder = Stream.builder();

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      String[] columnNames = IntStream.rangeClosed(1, columnCount).mapToObj(a -> {
        try {
          return resultSetMetaData.getColumnName(a);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).toArray(String[]::new);

      while (resultSet.next()) {
        Map<String, Object> row = new HashMap<>();

        for (int i = 0; i < columnCount; i++) {
          String columnName = columnNames[i];
          Object columnValue = resultSet.getObject(i + 1);
          logNode.info(String.format("setting col: %s = %s", columnName, columnValue));
          row.put(columnName, columnValue);
        }

        builder.accept(row);
      }
    }

    return builder.build();
  }

  public long executeStatement(String jdbcUrl, String userName, String password, String sql) throws SQLException {
    logNode.info(String.format("executeStatement: %s, %s, %s", jdbcUrl, userName, sql));

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      return preparedStatement.executeUpdate();
    }
  }
}

package databaseconnector.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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

  public List<IMendixObject> executeQuery(String jdbcUrl, String userName, String password, String entityName, String sql,
      IContext context) throws SQLException {
    List<IMendixObject> resultList = new ArrayList<>();

    executeQuery(jdbcUrl, userName, password, sql, context, row -> {
      IMendixObject obj = objectInstantiator.instantiate(context, entityName);
      row.stream().forEach(c -> obj.setValue(context, c.name, c.value));
      resultList.add(obj);
      logNode.info("obj: " + obj);
    });

    logNode.info(String.format("List: %d", resultList.size()));
    return resultList;
  }

  public String executeQueryToJson(String jdbcUrl, String userName, String password, String sql, IContext context) throws SQLException {
    return null;
  }

  public void executeQuery(String jdbcUrl, String userName, String password, String sql,
      IContext context, Consumer<List<Column>> consumer) throws SQLException {
    logNode.info(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

    // TODO: make sure user doesn't crash runtime due to retrieving too many records
    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();

      while (resultSet.next()) {
        ArrayList<Column> row = new ArrayList<Column>();

        for (int i = 0; i < columnCount; i++) {
          String columnName = resultSetMetaData.getColumnName(i + 1);
          Object columnValue = resultSet.getObject(i + 1);
          logNode.info(String.format("setting col: %s = %s", columnName, columnValue));
          row.add(new Column(columnName, columnValue));
        }

        consumer.accept(row);
      }
    }
  }

  public class Column {
    public String name;
    public Object value;

    public Column(String name, Object value) {
      this.name = name;
      this.value = value;
    }
  }

  public long executeStatement(String jdbcUrl, String userName, String password, String sql) throws SQLException {
    logNode.info(String.format("executeStatement: %s, %s, %s", jdbcUrl, userName, sql));

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
      return preparedStatement.executeUpdate();
    }
  }
}

package databaseconnector.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;

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
    Stream<Map<String, Object>> stream = executeQuery(jdbcUrl, userName, password, sql);
    Stream<JSONObject> jsonObjects = stream.map(JSONObject::new);

    return new JSONArray(jsonObjects.collect(Collectors.toList())).toString();
  }

  public boolean getResultSet(ResultSet rs) throws SQLException {
    return rs.next();
  }

  public Stream<Map<String, Object>> executeQuery(String jdbcUrl, String userName, String password, String sql) throws SQLException {
    logNode.info(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      ColumnNameInfoGenerator columnNameInfo = new ColumnNameInfoGenerator(resultSetMetaData);
      List<ColumnInfo> columnNames = IntStream.rangeClosed(1, columnCount)
          .mapToObj(columnNameInfo::getColumnInfo).collect(Collectors.toList());
      Stream<ResultSet> resultSetStream = ResultSetStream.stream(resultSet);
      Function<ResultSet, Map<String, Object>> toRowMap = rs -> {
        Function<ColumnInfo, Object> getValue = columnInfo -> {
          try {
            Object columnValue = rs.getObject(columnInfo.getIndex());
            logNode.info(String.format("setting col: %s = %s", columnInfo.getName(), columnValue));
            return columnValue;
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        };

        return columnNames.stream().collect(Collectors.toMap(ColumnInfo::getName, getValue));
      };

      Stream<Map<String, Object>> rowStream = resultSetStream.map(toRowMap);

      // Force the stream to read the whole ResultSet, so that the connection can be closed.
      return rowStream.collect(Collectors.toList()).stream();
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

package databaseconnector.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

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

  public Stream<IMendixObject> executeQuery(String jdbcUrl, String userName, String password, IMetaObject metaObject, String sql,
      IContext context) throws SQLException {
    String entityName = metaObject.getName();
    Function<Map<String, Object>, IMendixObject> toMendixObject = columns -> {
      IMendixObject obj = objectInstantiator.instantiate(context, entityName);
      columns.forEach((n, v) -> obj.setValue(context, n, v));
      logNode.info("obj: " + obj);
      return obj;
    };

    Collection<? extends IMetaPrimitive> metaPrimitives = metaObject.getMetaPrimitives();
    List<PrimitiveType> primitiveTypes = metaPrimitives.stream().map(mp -> mp.getType()).collect(Collectors.toList());
    Stream<Map<String,Object>> stream = executeQuery(jdbcUrl, userName, password, primitiveTypes, sql);

    return stream.map(toMendixObject);
  }

  public Stream<Map<String, Object>> executeQuery(String jdbcUrl, String userName, String password, List<PrimitiveType> primitiveTypes, String sql) throws SQLException {
    logNode.info(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetReader resultSetReader = new ResultSetReader(resultSet, primitiveTypes);

      return resultSetReader.readAll().stream();
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

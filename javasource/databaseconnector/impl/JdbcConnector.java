package databaseconnector.impl;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
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
    Function<Map<String, Optional<Object>>, IMendixObject> toMendixObject = columns -> {

      IMendixObject obj = objectInstantiator.instantiate(context, entityName);
      BiConsumer<String, Optional<Object>> setMemberValue = (name, value) -> {
        PrimitiveType primitiveType = metaObject.getMetaPrimitive(name).getType();
        // convert to suitable type
        Function<Object, Object> toSuitableValue = toSuitableValue(primitiveType);
        // for Boolean type, convert null to default false
        Supplier<Object> defaultValue = () -> primitiveType == PrimitiveType.Boolean ? Boolean.FALSE : null;
        // apply two functions declared above
        Object convertedValue = value.map(toSuitableValue).orElseGet(defaultValue);
        // update object with converted value
        obj.setValue(context, name, convertedValue);
      };
      columns.forEach(setMemberValue);
      logNode.trace("obj: " + obj);
      return obj;
    };

    return executeQuery(jdbcUrl, userName, password, metaObject, sql).map(toMendixObject);
  }

  private Function<Object, Object> toSuitableValue(final PrimitiveType type) {
    return v -> type == PrimitiveType.Binary ? new ByteArrayInputStream((byte[]) v) : v;
  }

  public Stream<Map<String, Optional<Object>>> executeQuery(String jdbcUrl, String userName, String password, IMetaObject metaObject, String sql) throws SQLException {
    logNode.info(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

    try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery()) {
      ResultSetReader resultSetReader = new ResultSetReader(resultSet, metaObject);

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

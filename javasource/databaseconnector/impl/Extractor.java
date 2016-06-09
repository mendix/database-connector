package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.stream.IntStream;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.interfaces.IExtractor;
import databaseconnector.interfaces.IObjectInstantiator;

public class Extractor<T> implements IExtractor<T> {

  private IContext context;
  private IObjectInstantiator objectInstantiator;
  private String entityName;
  private ResultSetMetaData resultSetMetaData;
  private int columnCount;
  private ILogNode logNode;

  public Extractor(IObjectInstantiator objectInstantiator, IContext context, String entityName,
      ResultSetMetaData resultSetMetaData, int columnCount, ILogNode logNode) {
    this.objectInstantiator = objectInstantiator;
    this.context = context;
    this.entityName = entityName;
    this.resultSetMetaData = resultSetMetaData;
    this.columnCount = columnCount;
    this.logNode = logNode;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T extract(ResultSet resultSet) throws SQLException {
    IMendixObject mendixObject = getMendixObject(context, entityName, objectInstantiator);

    IntStream.rangeClosed(1, columnCount).forEach(col ->
    mendixObject.setValue(context, getColName(resultSet, col), getColValue(resultSet, col)));
    logNode.info("MendixObject: " + mendixObject);
    return (T) mendixObject;
  }

  private String getColName(ResultSet rs, int columnNumber)  {
    try {
      return resultSetMetaData.getColumnName(columnNumber);
    } catch (SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  private Object getColValue(ResultSet rs, int columnNumber)  {
    try {
      return rs.getObject(columnNumber);
    } catch (SQLException sqle) {
      throw new RuntimeException(sqle);
    }
  }

  private IMendixObject getMendixObject(IContext context, String entityName, IObjectInstantiator obj) {
    return obj.instantiate(context, entityName);
  }
}

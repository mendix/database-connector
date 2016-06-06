package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.mendix.logging.ILogNode;

public class ResultSetStream implements Iterator<ResultSet> {
  private ResultSet resultSet;
  private ILogNode logNode;
  private List<ColumnInfo> columnNames;

  public ResultSetStream(ResultSet resultSet, ILogNode logNode) {
    this.resultSet = resultSet;
    this.logNode = logNode;

    try {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      ColumnNameInfoGenerator columnNameInfoGenerator = new ColumnNameInfoGenerator(resultSetMetaData);
      columnNames = IntStream.rangeClosed(1, columnCount)
          .mapToObj(columnNameInfoGenerator::getColumnInfo).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Map<String, Object>> toList() throws SQLException {
    return stream().map(a -> getRowResult(columnNames)).collect(Collectors.toList());
  }

  @Override
  public ResultSet next() {
    return resultSet;
  }

  @Override
  public boolean hasNext() {
    try {
      return resultSet.next();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Stream<ResultSet> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED), false);
  }

  private Map<String, Object> getRowResult(List<ColumnInfo> columnNames) {
    return columnNames.stream().collect(Collectors.toMap(ColumnInfo::getName, this::getColumnResult));
  }

  private Object getColumnResult(ColumnInfo columnInfo) {
    try {
      Object columnValue = resultSet.getObject(columnInfo.getIndex());
      logNode.info(String.format("setting col: %s = %s", columnInfo.getName(), columnValue));
      return columnValue;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

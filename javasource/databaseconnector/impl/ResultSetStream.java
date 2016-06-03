package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ResultSetStream implements Iterator<ResultSet> {
  private ResultSet resultSet;

  public ResultSetStream(ResultSet resultSet) {
    this.resultSet = resultSet;
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

  public static Stream<ResultSet> stream(ResultSet resultSet) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetStream(resultSet), Spliterator.ORDERED), false);
  }
}

package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import databaseconnector.interfaces.IExtractor;

public class ResultSetIterator<T> implements Iterator<T>, AutoCloseable {
  private final ResultSet resultSet;
  private final IExtractor<T> extractor;

  public ResultSetIterator(final ResultSet resultSet, final IExtractor<T> extractor) {
    this.extractor = extractor;
    this.resultSet = resultSet;
  }

  public static <T> Stream<T> stream(
      final ResultSet resultSet,
      final IExtractor<T> extractor) {
    ResultSetIterator<T> iterator = new ResultSetIterator<T>(resultSet, extractor);
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
        .onClose(() -> {
          try {
            iterator.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  @Override
  public boolean hasNext() {
    try {
      return resultSet.next();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T next() {
    try {
      return extractor.extract(resultSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    resultSet.close();
  }
}

package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

/**
 * ResultSetIterator implements {@link Iterator} interface. It wraps {@link ResultSet} into a stream for more convenient usage. Along
 * with that, it provides information about columns of the given result set.
 */
public class ResultSetIterator implements Iterator<ResultSet> {

  private final ResultSet resultSet;
  private final List<ColumnInfo> columnInfos;
  private final List<PrimitiveType> primitiveTypes;
  
  public ResultSetIterator(final ResultSet resultSet, final List<PrimitiveType> primitiveTypes) {
    this.resultSet = resultSet;
    this.primitiveTypes = primitiveTypes;
    this.columnInfos = createColumnInfos(resultSet);
  }

  private List<ColumnInfo> createColumnInfos(final ResultSet resultSet) {
    try {
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      final int columnCount = resultSetMetaData.getColumnCount();
      return IntStream.rangeClosed(1, columnCount).mapToObj(this::getColumnInfo).collect(Collectors.toList());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private ColumnInfo getColumnInfo(int index) {
    try {
      return new ColumnInfo(index, resultSet.getMetaData().getColumnName(index), primitiveTypes.get(index - 1));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  public Stream<ResultSet> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED), false);
  }

  public Stream<ColumnInfo> getColumnInfos() {
    return columnInfos.stream();
  }

}

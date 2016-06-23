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

import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;

/**
 * ResultSetIterator implements {@link Iterator} interface. It wraps {@link ResultSet} into a stream for more convenient usage. Along
 * with that, it provides information about columns of the given result set.
 */
public class ResultSetIterator implements Iterator<ResultSet> {

  private final ResultSet resultSet;
  private final List<ColumnInfo> columnInfos;
  private final IMetaObject metaObject;

  public ResultSetIterator(final ResultSet resultSet, final IMetaObject metaObject) {
    this.resultSet = resultSet;
    this.metaObject = metaObject;
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
    final String columnName;

    try {
      columnName = resultSet.getMetaData().getColumnName(index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    final IMetaPrimitive primitive = metaObject.getMetaPrimitive(columnName);

    if (primitive == null)
      throw new RuntimeException(String.format("The entity type '%s' does not contain the primitive '%s' as specified in the query.",
          metaObject.getName(),
          columnName));

    return new ColumnInfo(index, columnName, primitive.getType());
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

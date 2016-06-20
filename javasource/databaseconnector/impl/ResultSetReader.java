package databaseconnector.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ResultSetReader converts a given instance of {@link ResultSet} into a list of instances of Map<String, Object>, with key for column name
 * and value for column value.
 */
public class ResultSetReader {
  private final ResultSetIterator rsIter;

  public ResultSetReader(final ResultSet resultSet) {
    this.rsIter = new ResultSetIterator(resultSet);
  }

  /**
   * Read all records into list of maps.
   *
   * @return list of records, where records are represented as map
   * @throws SQLException
   */
  public List<Map<String, Object>> readAll() throws SQLException {
    // Force the stream to read the whole ResultSet, so that the connection can be closed.
    return rsIter.stream().map(rs -> getRowResult(rs)).collect(Collectors.toList());
  }

  private Map<String, Object> getRowResult(final ResultSet rs) {
    return rsIter.getColumnInfos().collect(Collectors.toMap(ColumnInfo::getName, curryGetColumnResult(rs)));
  }

  private Function<ColumnInfo, Object> curryGetColumnResult(final ResultSet rs) {
    return ci -> getColumnResult(rs, ci);
  }

  private Object getColumnResult(final ResultSet rs, final ColumnInfo columnInfo) {
    try {
      final Object columnValue = rs.getObject(columnInfo.getIndex());
      return columnValue;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

package databaseconnector.impl;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;

import com.mendix.systemwideinterfaces.core.meta.IMetaObject;

/**
 * ResultSetReader converts a given instance of {@link ResultSet} into a list of instances of Map<String, Object>, with key for column name
 * and value for column value.
 */
public class ResultSetReader {
  private final ResultSetIterator rsIter;
  private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  public ResultSetReader(final ResultSet resultSet, final IMetaObject metaObject) {
    this.rsIter = new ResultSetIterator(resultSet, metaObject);
  }

  /**
   * Read all records into list of maps.
   *
   * @return list of records, where records are represented as map
   * @throws SQLException
   */
  public List<Map<String, Optional<Object>>> readAll() throws SQLException {
    // Force the stream to read the whole ResultSet, so that the connection can be closed.
    return rsIter.stream().map(this::getRowResult).collect(toList());
  }

  /**
   * The Optional type for value is used because Collectors.toMap does not accept null for a value.
   */
  private Map<String, Optional<Object>> getRowResult(final ResultSet rs) {
    return rsIter.getColumnInfos().collect(toMap(ColumnInfo::getName, curryGetColumnResult(rs)));
  }

  private Function<ColumnInfo, Optional<Object>> curryGetColumnResult(final ResultSet rs) {
    return ci -> getColumnResult(rs, ci);
  }
  
  private Optional<Object> getColumnResult(final ResultSet rs, final ColumnInfo columnInfo) {
    try {
      final int columnIndex = columnInfo.getIndex();
      Object columnValue = null;
      switch (columnInfo.getType()) {
        case Integer:
          columnValue = rs.getInt(columnIndex);
          break;
        case AutoNumber:
        case Long:
          columnValue = rs.getLong(columnIndex);
          break;
        case DateTime:
          Timestamp timeStamp = rs.getTimestamp(columnIndex, calendar);
          columnValue = (timeStamp != null) ? new Date(timeStamp.getTime()) : null;
          break;
        case Boolean:
          columnValue = rs.getBoolean(columnIndex);
          break;
        case Decimal:
          columnValue = rs.getBigDecimal(columnIndex);
          break;
        case Enum:
        case String:
          columnValue = rs.getString(columnIndex);
          break;
        case Binary:
          columnValue = rs.getBytes(columnIndex);
          break;
      }
      return rs.wasNull() ? Optional.empty() : Optional.ofNullable(columnValue);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

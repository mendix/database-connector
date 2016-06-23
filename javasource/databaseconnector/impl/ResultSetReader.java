package databaseconnector.impl;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ResultSetReader converts a given instance of {@link ResultSet} into a list of instances of Map<String, Object>, with key for column name
 * and value for column value.
 */
public class ResultSetReader {
  private final ResultSetIterator rsIter;
  private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

  public ResultSetReader(final ResultSet resultSet, final List<PrimitiveType> primitiveTypes) {
    this.rsIter = new ResultSetIterator(resultSet, primitiveTypes);
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
      int index = columnInfo.getIndex();
      Object columnValue = null;
      switch (columnInfo.getType()) {
        case Integer:
          columnValue = rs.getInt(index);
          break;
        case AutoNumber:
        case Long:
          columnValue = rs.getLong(index);
          break;
        case DateTime:
          Timestamp timeStamp = rs.getTimestamp(index, calendar);
          columnValue = (timeStamp != null) ? new Date(timeStamp.getTime()) : null;
          break;
        case Boolean:
          columnValue = rs.getBoolean(index);
          break;
        case Decimal:
          columnValue = rs.getBigDecimal(index);
          break;
        case Float:
        case Currency:
          columnValue = rs.getDouble(index);
          break;
        case HashString:
        case Enum:
        case String:
          columnValue = rs.getString(index);
          break;
        case Binary:
          try (InputStream inputStream = rs.getBinaryStream(index)) {
            columnValue = inputStream;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          break;
      }
      return rs.wasNull() ? null : columnValue;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

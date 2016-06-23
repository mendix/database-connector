package databaseconnectortest.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import databaseconnector.impl.ResultSetReader;
import org.junit.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.*;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;

public class ResultSetReaderTest {

  private final List<IMetaPrimitive.PrimitiveType> types = Arrays.asList(String, String, String);

  private ResultSet mockResultSet() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(3);
    when(md.getColumnName(1)).thenReturn("a");
    when(md.getColumnName(2)).thenReturn("b");
    when(md.getColumnName(3)).thenReturn("c");
    when(rs.getMetaData()).thenReturn(md);
    return rs;
  }

  @Test
  public void testReadAll_Empty() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(false);
    final ResultSetReader rsr = new ResultSetReader(rs, types);
    assertTrue(rsr.readAll().isEmpty());
  }

  @Test
  public void testReadAll_OneRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn("a1");
    when(rs.getString(2)).thenReturn("b1");
    when(rs.getString(3)).thenReturn("c1");

    final ResultSetReader rsr = new ResultSetReader(rs, types);
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Object> record = records.get(0);
    assertEquals(3, record.size());
    assertEquals("a1", record.get("a"));
    assertEquals("b1", record.get("b"));
    assertEquals("c1", record.get("c"));
  }

  @Test
  public void testReadAll_TwoRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString(1)).thenReturn("a1", "a2");
    when(rs.getString(2)).thenReturn("b1", "b2");
    when(rs.getString(3)).thenReturn("c1", "c2");

    final ResultSetReader rsr = new ResultSetReader(rs, types);
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(2, records.size());

    final Map<String, Object> record1 = records.get(0);
    assertEquals(3, record1.size());
    assertEquals("a1", record1.get("a"));
    assertEquals("b1", record1.get("b"));
    assertEquals("c1", record1.get("c"));

    final Map<String, Object> record2 = records.get(1);
    assertEquals(3, record2.size());
    assertEquals("a2", record2.get("a"));
    assertEquals("b2", record2.get("b"));
    assertEquals("c2", record2.get("c"));
  }

  @Test
  public void testAllTypes() throws SQLException {

    List<IMetaPrimitive.PrimitiveType> allTypes = Arrays.asList(Integer, AutoNumber, Long,
                                                                Boolean, Decimal, Float, Currency,
                                                                HashString, Enum, String, Binary, DateTime);

    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(allTypes.size());
    when(md.getColumnName(1)).thenReturn("Integer");
    when(md.getColumnName(2)).thenReturn("AutoNumber");
    when(md.getColumnName(3)).thenReturn("Long");
    when(md.getColumnName(4)).thenReturn("Boolean");
    when(md.getColumnName(5)).thenReturn("Decimal");
    when(md.getColumnName(6)).thenReturn("Float");
    when(md.getColumnName(7)).thenReturn("Currency");
    when(md.getColumnName(8)).thenReturn("HashString");
    when(md.getColumnName(9)).thenReturn("Enum");
    when(md.getColumnName(10)).thenReturn("String");
    when(md.getColumnName(11)).thenReturn("Binary");
    when(md.getColumnName(12)).thenReturn("DateTime");
    when(rs.getMetaData()).thenReturn(md);

    when(rs.next()).thenReturn(true, false);
    when(rs.getInt(1)).thenReturn(1);
    when(rs.getLong(2)).thenReturn(2L);
    when(rs.getLong(3)).thenReturn(3L);
    when(rs.getBoolean(4)).thenReturn(true);
    when(rs.getBigDecimal(5)).thenReturn(new BigDecimal("123"));
    when(rs.getDouble(6)).thenReturn(4.0);
    when(rs.getDouble(7)).thenReturn(5.0);
    when(rs.getString(8)).thenReturn("A0");
    when(rs.getString(9)).thenReturn("A1");
    when(rs.getString(10)).thenReturn("A2");
    when(rs.getBytes(11)).thenReturn("привет мир".getBytes());
    when(rs.getTimestamp(Mockito.eq(12), Mockito.any(Calendar.class))).thenReturn(new Timestamp(0L));

    final ResultSetReader rsr = new ResultSetReader(rs, allTypes);
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());

    final Map<String, Object> record = records.get(0);
    assertEquals(allTypes.size(), record.size());
    assertEquals(1, record.get("Integer"));
    assertEquals(2L, record.get("AutoNumber"));
    assertEquals(3L, record.get("Long"));
    assertEquals(true, record.get("Boolean"));
    assertEquals(new BigDecimal("123"), record.get("Decimal"));
    assertEquals(4.0, record.get("Float"));
    assertEquals(5.0, record.get("Currency"));
    assertEquals("A0", record.get("HashString"));
    assertEquals("A1", record.get("Enum"));
    assertEquals("A2", record.get("String"));
    assertEquals("привет мир", new String((byte[]) record.get("Binary")));
    assertEquals(new Date(0L), record.get("DateTime"));
  }

  @Test
  public void testNullResultSet() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    when(rs.wasNull()).thenReturn(true);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(1);
    when(md.getColumnName(1)).thenReturn("String");
    when(rs.getMetaData()).thenReturn(md);

    when(rs.next()).thenReturn(true, false);
    when(rs.getString("String")).thenReturn(null);

    final ResultSetReader rsr = new ResultSetReader(rs, Arrays.asList(String));
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Object> record = records.get(0);
    assertNull(record.get("String"));
  }

  @Test
  public void testNullResult() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(1);
    when(md.getColumnName(1)).thenReturn("String");
    when(rs.getMetaData()).thenReturn(md);

    when(rs.next()).thenReturn(true, false);
    when(rs.getString("String")).thenReturn(null);

    final ResultSetReader rsr = new ResultSetReader(rs, Arrays.asList(String));
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Object> record = records.get(0);
    assertNull(record.get("String"));
  }

}

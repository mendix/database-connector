package databaseconnectortest.test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import databaseconnector.impl.ResultSetReader;

public class ResultSetReaderTest {

  private final IMetaPrimitive integerPrimitive = mockPrimitive(PrimitiveType.Integer);
  private final IMetaPrimitive autoNumberPrimitive = mockPrimitive(PrimitiveType.AutoNumber);
  private final IMetaPrimitive longPrimitive = mockPrimitive(PrimitiveType.Long);
  private final IMetaPrimitive booleanPrimitive = mockPrimitive(PrimitiveType.Boolean);
  private final IMetaPrimitive decimalPrimitive = mockPrimitive(PrimitiveType.Decimal);
  @SuppressWarnings("deprecation")
  private final IMetaPrimitive floatPrimitive = mockPrimitive(PrimitiveType.Float);
  @SuppressWarnings("deprecation")
  private final IMetaPrimitive currencyPrimitive = mockPrimitive(PrimitiveType.Currency);
  private final IMetaPrimitive hashStringPrimitive = mockPrimitive(PrimitiveType.HashString);
  private final IMetaPrimitive enumPrimitive = mockPrimitive(PrimitiveType.Enum);
  private final IMetaPrimitive stringPrimitive = mockPrimitive(PrimitiveType.String);
  private final IMetaPrimitive binaryPrimitive = mockPrimitive(PrimitiveType.Binary);
  private final IMetaPrimitive dateTimePrimitive = mockPrimitive(PrimitiveType.DateTime);
  private final IMetaObject metaObjectWithStrings = mockMetaObjectWithStrings();

  private IMetaObject mockMetaObjectWithStrings() {
    final IMetaObject metaObject = mock(IMetaObject.class);
    when(metaObject.getMetaPrimitive("a")).thenReturn(stringPrimitive);
    when(metaObject.getMetaPrimitive("b")).thenReturn(stringPrimitive);
    when(metaObject.getMetaPrimitive("c")).thenReturn(stringPrimitive);

    return metaObject;
  }

  private IMetaPrimitive mockPrimitive(PrimitiveType primitiveType) {
    final IMetaPrimitive primitive = mock(IMetaPrimitive.class);
    when(primitive.getType()).thenReturn(primitiveType);

    return primitive;
  }

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
    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStrings);
    assertTrue(rsr.readAll().isEmpty());
  }

  @Test
  public void testReadAll_OneRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn("a1");
    when(rs.getString(2)).thenReturn("b1");
    when(rs.getString(3)).thenReturn("c1");

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStrings);
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

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStrings);
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
    final IMetaObject metaObject = mock(IMetaObject.class);
    when(metaObject.getMetaPrimitive("Integer")).thenReturn(integerPrimitive);
    when(metaObject.getMetaPrimitive("AutoNumber")).thenReturn(autoNumberPrimitive);
    when(metaObject.getMetaPrimitive("Long")).thenReturn(longPrimitive);
    when(metaObject.getMetaPrimitive("Boolean")).thenReturn(booleanPrimitive);
    when(metaObject.getMetaPrimitive("Decimal")).thenReturn(decimalPrimitive);
    when(metaObject.getMetaPrimitive("Float")).thenReturn(floatPrimitive);
    when(metaObject.getMetaPrimitive("Currency")).thenReturn(currencyPrimitive);
    when(metaObject.getMetaPrimitive("HashString")).thenReturn(hashStringPrimitive);
    when(metaObject.getMetaPrimitive("Enum")).thenReturn(enumPrimitive);
    when(metaObject.getMetaPrimitive("String")).thenReturn(stringPrimitive);
    when(metaObject.getMetaPrimitive("Binary")).thenReturn(binaryPrimitive);
    when(metaObject.getMetaPrimitive("DateTime")).thenReturn(dateTimePrimitive);

    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(12);
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

    final ResultSetReader rsr = new ResultSetReader(rs, metaObject);
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());

    final Map<String, Object> record = records.get(0);
    assertEquals(12, record.size());
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

    final IMetaObject metaObject = mock(IMetaObject.class);
    when(metaObject.getMetaPrimitive("String")).thenReturn(stringPrimitive);

    final ResultSetReader rsr = new ResultSetReader(rs, metaObject);
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

    final IMetaObject metaObject = mock(IMetaObject.class);
    when(metaObject.getMetaPrimitive("String")).thenReturn(stringPrimitive);

    final ResultSetReader rsr = new ResultSetReader(rs, metaObject);
    final List<Map<String, Object>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Object> record = records.get(0);
    assertNull(record.get("String"));
  }

}

package databaseconnectortest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mockito;

import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import databaseconnector.impl.ResultSetReader;

public class ResultSetReaderTest {

  private final IMetaPrimitive integerPrimitive = mockMetaPrimitive(PrimitiveType.Integer);
  private final IMetaPrimitive autoNumberPrimitive = mockMetaPrimitive(PrimitiveType.AutoNumber);
  private final IMetaPrimitive longPrimitive = mockMetaPrimitive(PrimitiveType.Long);
  private final IMetaPrimitive booleanPrimitive = mockMetaPrimitive(PrimitiveType.Boolean);
  private final IMetaPrimitive decimalPrimitive = mockMetaPrimitive(PrimitiveType.Decimal);
  @SuppressWarnings("deprecation")
  private final IMetaPrimitive floatPrimitive = mockMetaPrimitive(PrimitiveType.Float);
  @SuppressWarnings("deprecation")
  private final IMetaPrimitive currencyPrimitive = mockMetaPrimitive(PrimitiveType.Currency);
  private final IMetaPrimitive hashStringPrimitive = mockMetaPrimitive(PrimitiveType.HashString);
  private final IMetaPrimitive enumPrimitive = mockMetaPrimitive(PrimitiveType.Enum);
  private final IMetaPrimitive stringPrimitive = mockMetaPrimitive(PrimitiveType.String);
  private final IMetaPrimitive binaryPrimitive = mockMetaPrimitive(PrimitiveType.Binary);
  private final IMetaPrimitive dateTimePrimitive = mockMetaPrimitive(PrimitiveType.DateTime);

  private final IMetaObject metaObjectWithAllPrimitives = mockMetaObjectWithAllPrimitives();
  private final IMetaObject metaObjectWithThreePrimitives = mockMetaObjectWithThreePrimitives();
  private final IMetaObject metaObjectWithStringPrimitive = mockMetaObjectWithStringPrimitive();

  private IMetaObject mockMetaObjectWithAllPrimitives() {
    final IMetaObject metaObject = mock(IMetaObject.class);
    final Collection<? extends IMetaPrimitive> allPrimitives = Arrays.asList(
        integerPrimitive,
        autoNumberPrimitive,
        longPrimitive,
        booleanPrimitive,
        decimalPrimitive,
        floatPrimitive,
        currencyPrimitive,
        hashStringPrimitive,
        enumPrimitive,
        stringPrimitive,
        binaryPrimitive,
        dateTimePrimitive
    );
    Mockito.<Collection<? extends IMetaPrimitive>>when(metaObject.getMetaPrimitives()).thenReturn(allPrimitives);
    return metaObject;
  }

  private IMetaObject mockMetaObjectWithThreePrimitives() {
    final IMetaObject metaObject = mock(IMetaObject.class);
    final Collection<? extends IMetaPrimitive> allPrimitives = Arrays.asList(
        integerPrimitive,
        booleanPrimitive,
        stringPrimitive
    );
    Mockito.<Collection<? extends IMetaPrimitive>>when(metaObject.getMetaPrimitives()).thenReturn(allPrimitives);
    return metaObject;
  }

  private IMetaObject mockMetaObjectWithStringPrimitive() {
    final IMetaObject metaObject = mock(IMetaObject.class);
    final Collection<? extends IMetaPrimitive> allPrimitives = Arrays.asList(stringPrimitive);
    Mockito.<Collection<? extends IMetaPrimitive>>when(metaObject.getMetaPrimitives()).thenReturn(allPrimitives);
    return metaObject;
  }

  private IMetaPrimitive mockMetaPrimitive(PrimitiveType primitiveType) {
    return mockMetaPrimitive(primitiveType.name(), primitiveType);
  }


  private IMetaPrimitive mockMetaPrimitive(String name, PrimitiveType type) {
    final IMetaPrimitive primitive = mock(IMetaPrimitive.class);
    when(primitive.getType()).thenReturn(type);
    when(primitive.getName()).thenReturn(name);
    return primitive;
  }

  private ResultSet mockResultSet() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(3);
    when(md.getColumnName(1)).thenReturn("Integer");
    when(md.getColumnName(2)).thenReturn("Boolean");
    when(md.getColumnName(3)).thenReturn("String");
    when(rs.getMetaData()).thenReturn(md);
    return rs;
  }

  @Test
  public void testReadAll_Empty() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(false);
    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithThreePrimitives);
    assertTrue(rsr.readAll().isEmpty());
  }

  @Test
  public void testReadAll_OneRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, false);
    when(rs.getInt(1)).thenReturn(1);
    when(rs.getBoolean(2)).thenReturn(true);
    when(rs.getString(3)).thenReturn("a");

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithThreePrimitives);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Optional<Object>> record = records.get(0);
    assertEquals(3, record.size());
    assertEquals(1, record.get("Integer").get());
    assertEquals(true, record.get("Boolean").get());
    assertEquals("a", record.get("String").get());
  }

  @Test
  public void testReadAll_OneRecordWithDifferentCasing() throws SQLException {
    final ResultSet rs = mock(ResultSet.class);
    final ResultSetMetaData md = mock(ResultSetMetaData.class);
    when(md.getColumnCount()).thenReturn(1);
    when(md.getColumnName(1)).thenReturn("StRiNg");
    when(rs.getMetaData()).thenReturn(md);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString(1)).thenReturn("a");

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStringPrimitive);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Optional<Object>> record = records.get(0);
    assertEquals(1, record.size());
    assertEquals("a", record.get("StRiNg").get());
  }

  @Test
  public void testReadAll_TwoRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getInt(1)).thenReturn(1, 2);
    when(rs.getBoolean(2)).thenReturn(true, false);
    when(rs.getString(3)).thenReturn("a", "b");

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithThreePrimitives);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(2, records.size());

    final Map<String, Optional<Object>> record1 = records.get(0);
    assertEquals(3, record1.size());
    assertEquals(1, record1.get("Integer").get());
    assertEquals(true, record1.get("Boolean").get());
    assertEquals("a", record1.get("String").get());

    final Map<String, Optional<Object>> record2 = records.get(1);
    assertEquals(3, record2.size());
    assertEquals(2, record2.get("Integer").get());
    assertEquals(false, record2.get("Boolean").get());
    assertEquals("b", record2.get("String").get());
  }

  @Test
  public void testAllTypes() throws SQLException {
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

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithAllPrimitives);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(1, records.size());

    final Map<String, Optional<Object>> record = records.get(0);
    assertEquals(12, record.size());
    assertEquals(1, record.get("Integer").get());
    assertEquals(2L, record.get("AutoNumber").get());
    assertEquals(3L, record.get("Long").get());
    assertEquals(true, record.get("Boolean").get());
    assertEquals(new BigDecimal("123"), record.get("Decimal").get());
    assertEquals(4.0, record.get("Float").get());
    assertEquals(5.0, record.get("Currency").get());
    assertEquals("A0", record.get("HashString").get());
    assertEquals("A1", record.get("Enum").get());
    assertEquals("A2", record.get("String").get());
    assertEquals("привет мир", new String((byte[]) record.get("Binary").get()));
    assertEquals(new Date(0L), record.get("DateTime").get());
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

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStringPrimitive);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Optional<Object>> record = records.get(0);
    assertFalse(record.get("String").isPresent());
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

    final ResultSetReader rsr = new ResultSetReader(rs, metaObjectWithStringPrimitive);
    final List<Map<String, Optional<Object>>> records = rsr.readAll();
    assertEquals(1, records.size());
    final Map<String, Optional<Object>> record = records.get(0);
    assertFalse(record.get("String").isPresent());
  }


}

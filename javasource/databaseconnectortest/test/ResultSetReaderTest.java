package databaseconnectortest.test;


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import databaseconnector.impl.ResultSetReader;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class ResultSetReaderTest {

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
    final ResultSetReader rsr = new ResultSetReader(rs);
    assertTrue(rsr.readAll().isEmpty());
  }

  @Test
  public void testReadAll_OneRecord() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, false);
    when(rs.getObject(1)).thenReturn("a1");
    when(rs.getObject(2)).thenReturn("b1");
    when(rs.getObject(3)).thenReturn("c1");

    final ResultSetReader rsr = new ResultSetReader(rs);
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
    when(rs.getObject(1)).thenReturn("a1", "a2");
    when(rs.getObject(2)).thenReturn("b1", "b2");
    when(rs.getObject(3)).thenReturn("c1", "c2");

    final ResultSetReader rsr = new ResultSetReader(rs);
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
}

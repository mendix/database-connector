package databaseconnectortest.test;

import databaseconnector.impl.ColumnInfo;
import databaseconnector.impl.ResultSetIterator;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;

import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.String;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ResultSetIteratorTest {

  private final Map<String, PrimitiveType> columnsTypes = Stream.of(
      new SimpleEntry<>("a", String),
      new SimpleEntry<>("b", String),
      new SimpleEntry<>("c", String)
  ).collect(Collectors.toMap(
      (e) -> e.getKey(),
      (e) -> e.getValue()));


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
  public void testColumnInfos() throws SQLException {
    final ResultSet rs = mockResultSet();
    final ResultSetIterator rsi = new ResultSetIterator(rs, columnsTypes);
    final Stream<ColumnInfo> infos = rsi.getColumnInfos();
    final List<String> actual = infos.map(ci -> ci.getIndex() + ":" + ci.getName()).collect(Collectors.toList());
    final List<String> expected = Arrays.asList("1:a", "2:b", "3:c");
    assertEquals(expected, actual);
  }

  @Test
  public void testHasNext_Empty() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(false);
    final ResultSetIterator rsi = new ResultSetIterator(rs, columnsTypes);
    assertFalse(rsi.hasNext());
  }

  @Test
  public void testStream_Empty() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(false);
    final ResultSetIterator rsi = new ResultSetIterator(rs, columnsTypes);
    assertEquals(0, rsi.stream().count());
  }

  @Test
  public void testStream_OneRow() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, false);
    final ResultSetIterator rsi = new ResultSetIterator(rs, columnsTypes);
    assertEquals(1, rsi.stream().count());
    verify(rs, times(2)).next();
  }

  @Test
  public void testStream_TwoRows() throws SQLException {
    final ResultSet rs = mockResultSet();
    when(rs.next()).thenReturn(true, true, false);
    final ResultSetIterator rsi = new ResultSetIterator(rs, columnsTypes);
    assertEquals(2, rsi.stream().count());
    verify(rs, times(3)).next();
  }
}

package databaseconnectortest.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.String;
import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.Boolean;

import databaseconnector.impl.JdbcConnector;
import databaseconnector.interfaces.ConnectionManager;
import databaseconnector.interfaces.ObjectInstantiator;

public class JdbcConnectorTest {
  private static final String jdbcUrl = "TestUrl";
  private static final String userName = "TestUserName";
  private static final String password = "TestPassword";
  private static final String sqlQuery = "TestSqlQuery";
  private static final String entityName = "TestEntityName";

  @Rule public MockitoRule rule = MockitoJUnit.rule();

  @Mock private IContext context;
  @Mock private ObjectInstantiator objectInstantiator;
  @Mock private ILogNode iLogNode;
  @Mock private Connection connection;
  @Mock private ConnectionManager connectionManager;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;
  @Mock private ResultSetMetaData resultSetMetaData;

  @InjectMocks private JdbcConnector jdbcConnector;

  private IMetaObject mockIMetaObject(SimpleEntry<String, IMetaPrimitive.PrimitiveType>... entries) {
    IMetaObject metaObject = mock(IMetaObject.class);
    when(metaObject.getName()).thenReturn(entityName);

    List<IMetaPrimitive> metaPrimitives = Arrays.asList(entries).stream().map(entry -> {
      IMetaPrimitive metaPrimitive = mock(IMetaPrimitive.class);
      when(metaPrimitive.getName()).thenReturn(entry.getKey());
      when(metaPrimitive.getType()).thenReturn(entry.getValue());
      return metaPrimitive;
    }).collect(Collectors.toList());

    Mockito.<Collection<? extends IMetaPrimitive>>when(metaObject.getMetaPrimitives()).thenReturn(metaPrimitives);
    return metaObject;
  }

  private SimpleEntry<String, IMetaPrimitive.PrimitiveType> entry(String name, IMetaPrimitive.PrimitiveType type) {
    return new SimpleEntry<>(name, type);
  }

  @Test public void testStatementCreationException() throws SQLException {
    Exception testException = new SQLException("Test Exception Text");

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenThrow(testException);

    try {
      jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(), sqlQuery, context);
      fail("An exception should occur!");
    } catch(SQLException sqlException) {}

    verify(connection).close();
    verify(preparedStatement, never()).close();
  }

  @Test public void testObjectInstantiatorException() throws SQLException {
    Exception testException = new IllegalArgumentException("Test Exception Text");

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.next()).thenReturn(true, false);
    when(objectInstantiator.instantiate(anyObject(), anyString())).thenThrow(testException);

    Stream<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(), sqlQuery, context);
    try {
     result.count();
     fail("An exception should occur!");
    } catch(IllegalArgumentException iae) {}

    verify(objectInstantiator).instantiate(context, entityName);
    verify(connection).close();
    verify(preparedStatement).close();
    verify(resultSet).close();
  }

  @Test public void testConnectionClose() throws SQLException {
    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

    Stream<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(), sqlQuery, context);

    assertEquals(0, result.count());

    verify(connection).close();
    verify(preparedStatement).close();
    verify(resultSet).close();
  }

  @Test public void testSomeResults() throws SQLException {
    IMendixObject resultObject = mock(IMendixObject.class);

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject);
    when(resultSetMetaData.getColumnName(anyInt())).thenReturn("a", "b");
    when(resultSetMetaData.getColumnCount()).thenReturn(2);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.getBoolean(anyInt())).thenReturn(true);
    when(resultSet.next()).thenReturn(true, true, true, true, false);

    IMetaObject metaObject = mockIMetaObject(entry("a", Boolean), entry("b", Boolean));
    Stream<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery, context);

    assertEquals(4, result.count());

    verify(objectInstantiator, times(4)).instantiate(context, entityName);
    verify(connectionManager).getConnection(jdbcUrl, userName, password);
    verify(connection).prepareStatement(sqlQuery);
    verify(resultSet, times(3)).getMetaData();
    verify(resultSetMetaData).getColumnCount();
    verify(resultSet, times(5)).next();
  }

  @Test public void testSomeResultsWithTwoColumns() throws SQLException {
    String columnName1 = "TestColumnName1";
    String columnName2 = "TestColumnName2";
    String row1Value1 = "TestRow1Value1";
    String row1Value2 = "TestRow1Value2";
    String row2Value1 = "TestRow2Value1";
    String row2Value2 = "TestRow2Value2";

    IMendixObject resultObject1 = mock(IMendixObject.class);
    IMendixObject resultObject2 = mock(IMendixObject.class);

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject1, resultObject2);
    when(resultSetMetaData.getColumnCount()).thenReturn(2);
    when(resultSetMetaData.getColumnName(1)).thenReturn(columnName1);
    when(resultSetMetaData.getColumnName(2)).thenReturn(columnName2);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.next()).thenReturn(true, true, false);
    when(resultSet.getString(1)).thenReturn(row1Value1, row2Value1);
    when(resultSet.getString(2)).thenReturn(row1Value2, row2Value2);

    IMetaObject metaObject = mockIMetaObject(entry(columnName1, String), entry(columnName2, String));
    Stream<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery, context);
    assertEquals(2, result.count());

    verify(resultObject1).setValue(context, columnName1, row1Value1);
    verify(resultObject1).setValue(context, columnName2, row1Value2);
    verify(resultObject2).setValue(context, columnName1, row2Value1);
    verify(resultObject2).setValue(context, columnName2, row2Value2);
    verify(objectInstantiator, times(2)).instantiate(context, entityName);
    verify(resultSet, times(3)).next();
  }

  @Test public void testNoResults() throws Exception {
    IMendixObject resultObject = mock(IMendixObject.class);

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject);
    when(resultSetMetaData.getColumnCount()).thenReturn(2);
    when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
    when(resultSet.next()).thenReturn(false);

    IMetaObject metaObject = mockIMetaObject(entry("a", String), entry("b", Boolean));
    Stream<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery, context);

    assertEquals(0, result.count());

    verify(objectInstantiator, never()).instantiate(context, entityName);
    verify(resultSet, times(1)).next();
  }

  /*
   * Tests for Execute statement
   */

  @Test public void exceptionOnPrepareStatementForExecuteStatement() throws SQLException {
    Exception testException = new SQLException("Test Exception Text");

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenThrow(testException);

    try {
      jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);
      fail("An exception should occur!");
    } catch(SQLException sqlException) {}

    verify(connection).close();
    verify(preparedStatement, never()).close();
    verify(preparedStatement, never()).executeUpdate();
  }

  @Test public void exceptionOnExecuteUpdate() throws SQLException {
    Exception testException = new SQLException("Test Exception Text");

    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenThrow(testException);

    try {
      jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);
      fail("An exception should occur!");
    } catch(SQLException sqlException) {}

    verify(connection).close();
    verify(preparedStatement).close();
    verify(preparedStatement).executeUpdate();
  }

  @Test public void testCloseResourcesForExecuteStatement() throws SQLException {
    when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeUpdate()).thenReturn(5);

    long result = jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);

    assertEquals(5, result);

    verify(connection).close();
    verify(preparedStatement).close();
  }
}

package databaseconnectortest.test;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.impl.JdbcConnector;
import databaseconnector.impl.PreparedStatementCreatorImpl;
import databaseconnector.interfaces.ConnectionManager;
import databaseconnector.interfaces.ObjectInstantiator;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.sql.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.Boolean;
import static com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType.String;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class JdbcConnectorTest {
	private static final String jdbcUrl = "TestUrl";
	private static final String userName = "TestUserName";
	private static final String password = "TestPassword";
	private static final String sqlQuery = "TestSqlQuery";
	private static final String entityName = "TestEntityName";

	@Rule
	public MockitoRule rule = MockitoJUnit.rule();

	@Mock
	private IContext context;
	@Mock
	private ObjectInstantiator objectInstantiator;
	@Mock
	private ILogNode iLogNode;
	@Mock
	private Connection connection;
	@Mock
	private ConnectionManager connectionManager;
	@Mock
	private PreparedStatement preparedStatement;
	@Mock
	private ResultSet resultSet;
	@Mock
	private ResultSetMetaData resultSetMetaData;
	@Mock
	private PreparedStatementCreatorImpl preparedStatementCreator;

	@InjectMocks
	private JdbcConnector jdbcConnector;

	private IMetaObject mockIMetaObject(SimpleEntry<String, IMetaPrimitive.PrimitiveType>... entries) {
		IMetaObject metaObject = mock(IMetaObject.class);
		when(metaObject.getName()).thenReturn(entityName);

		final Collection<IMetaPrimitive> primitives = new ArrayList<>();

		Arrays.asList(entries).forEach(entry -> {
			IMetaPrimitive metaPrimitive = mock(IMetaPrimitive.class);
			when(metaPrimitive.getName()).thenReturn(entry.getKey());
			when(metaPrimitive.getType()).thenReturn(entry.getValue());
			when(metaObject.getMetaPrimitive(entry.getKey())).thenReturn(metaPrimitive);
			primitives.add(metaPrimitive);
		});

		Mockito.<Collection<? extends IMetaPrimitive>>when(metaObject.getMetaPrimitives()).thenReturn(primitives);
		return metaObject;
	}

	private SimpleEntry<String, IMetaPrimitive.PrimitiveType> entry(String name, IMetaPrimitive.PrimitiveType type) {
		return new SimpleEntry<>(name, type);
	}

	@Test(expected = SQLException.class)
	public void testStatementCreationException() throws SQLException, DatabaseConnectorException {
		Exception testException = new SQLException("Test Exception Text");

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenThrow(testException);

		try {
			jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(), sqlQuery, context);
			fail("An exception should occur!");
		} finally {
			verify(connection).close();
			verify(preparedStatement, never()).close();
		}
	}

	@Test
	public void testObjectInstantiatorException() throws SQLException, DatabaseConnectorException {
		Exception testException = new IllegalArgumentException("Test Exception Text");

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeQuery()).thenReturn(resultSet);
		when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
		when(resultSet.next()).thenReturn(true, false);
		when(objectInstantiator.instantiate(anyObject(), anyString())).thenThrow(testException);

		try {
			jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(), sqlQuery, context);
			fail("An exception should occur!");
		} catch (IllegalArgumentException iae) {
		}

		verify(objectInstantiator).instantiate(context, entityName);
		verify(connection).close();
		verify(preparedStatement).close();
		verify(resultSet).close();
	}

	@Test
	public void testConnectionClose() throws SQLException, DatabaseConnectorException {
		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeQuery()).thenReturn(resultSet);
		when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

		List<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, mockIMetaObject(),
				sqlQuery, context);

		assertEquals(0, result.size());

		verify(connection).close();
		verify(preparedStatement).close();
		verify(resultSet).close();
	}

	@Test
	public void testSomeResults() throws SQLException, DatabaseConnectorException {
		IMendixObject resultObject = mock(IMendixObject.class);

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeQuery()).thenReturn(resultSet);
		when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject);
		when(resultSetMetaData.getColumnName(anyInt())).thenReturn("a", "b");
		when(resultSetMetaData.getColumnCount()).thenReturn(2);
		when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
		when(resultSet.getBoolean(anyInt())).thenReturn(true);
		when(resultSet.next()).thenReturn(true, true, true, true, false);

		IMetaObject metaObject = mockIMetaObject(entry("a", Boolean), entry("b", Boolean));
		List<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery,
				context);

		assertEquals(4, result.size());

		verify(objectInstantiator, times(4)).instantiate(context, entityName);
		verify(connectionManager).getConnection(jdbcUrl, userName, password);
		verify(preparedStatementCreator).create(sqlQuery, connection);
		verify(resultSet, times(3)).getMetaData();
		verify(resultSetMetaData).getColumnCount();
		verify(resultSet, times(5)).next();
	}

	@Test
	public void testSomeResultsWithTwoColumns() throws SQLException, DatabaseConnectorException {
		String columnName1 = "TestColumnName1";
		String columnName2 = "TestColumnName2";
		String row1Value1 = "TestRow1Value1";
		String row1Value2 = "TestRow1Value2";
		String row2Value1 = "TestRow2Value1";
		String row2Value2 = "TestRow2Value2";

		IMendixObject resultObject1 = mock(IMendixObject.class);
		IMendixObject resultObject2 = mock(IMendixObject.class);

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
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
		List<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery,
				context);
		assertEquals(2, result.size());

		verify(resultObject1).setValue(context, columnName1, row1Value1);
		verify(resultObject1).setValue(context, columnName2, row1Value2);
		verify(resultObject2).setValue(context, columnName1, row2Value1);
		verify(resultObject2).setValue(context, columnName2, row2Value2);
		verify(objectInstantiator, times(2)).instantiate(context, entityName);
		verify(resultSet, times(3)).next();
	}

	@Test
	public void testResultForBoolean() throws SQLException, DatabaseConnectorException {
		IMendixObject resultObject = mock(IMendixObject.class);

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeQuery()).thenReturn(resultSet);
		when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject);

		when(resultSetMetaData.getColumnCount()).thenReturn(1);
		when(resultSetMetaData.getColumnName(1)).thenReturn("Boolean");
		when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
		when(resultSet.next()).thenReturn(true, false);

		// As Mockito does not allow to return null, we should not mock getting Boolean
		// when(resultSet.getBoolean(1)).thenReturn(null);

		IMetaObject metaObject = mockIMetaObject(entry("Boolean", Boolean), entry("String", String));
		List<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery,
				context);
		assertEquals(1, result.size());

		verify(resultObject).setValue(context, "Boolean", false);
		verify(objectInstantiator, times(1)).instantiate(context, entityName);
		verify(resultSet, times(2)).next();
	}

	@Test
	public void testNoResults() throws Exception {
		IMendixObject resultObject = mock(IMendixObject.class);

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeQuery()).thenReturn(resultSet);
		when(objectInstantiator.instantiate(anyObject(), anyString())).thenReturn(resultObject);
		when(resultSetMetaData.getColumnCount()).thenReturn(2);
		when(resultSetMetaData.getColumnName(1)).thenReturn("a");
		when(resultSetMetaData.getColumnName(2)).thenReturn("b");
		when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
		when(resultSet.next()).thenReturn(false);

		IMetaObject metaObject = mockIMetaObject(entry("a", String), entry("b", Boolean));
		List<IMendixObject> result = jdbcConnector.executeQuery(jdbcUrl, userName, password, metaObject, sqlQuery,
				context);

		assertEquals(0, result.size());

		verify(objectInstantiator, never()).instantiate(context, entityName);
		verify(resultSet, times(1)).next();
	}

	/*
	 * Tests for Execute statement
	 */

	@Test
	public void exceptionOnPrepareStatementForExecuteStatement() throws SQLException {
		Exception testException = new SQLException("Test Exception Text");

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenThrow(testException);

		try {
			jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);
			fail("An exception should occur!");
		} catch (SQLException sqlException) {
		}

		verify(connection).close();
		verify(preparedStatement, never()).close();
		verify(preparedStatement, never()).executeUpdate();
	}

	@Test
	public void exceptionOnExecuteUpdate() throws SQLException {
		Exception testException = new SQLException("Test Exception Text");

		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeUpdate()).thenThrow(testException);

		try {
			jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);
			fail("An exception should occur!");
		} catch (SQLException sqlException) {
		}

		verify(connection).close();
		verify(preparedStatement).close();
		verify(preparedStatement).executeUpdate();
	}

	@Test
	public void testCloseResourcesForExecuteStatement() throws SQLException {
		when(connectionManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
		when(preparedStatementCreator.create(anyString(), eq(connection))).thenReturn(preparedStatement);
		when(preparedStatement.executeUpdate()).thenReturn(5);

		long result = jdbcConnector.executeStatement(jdbcUrl, userName, password, sqlQuery);

		assertEquals(5, result);

		verify(connection).close();
		verify(preparedStatement).close();
	}
}

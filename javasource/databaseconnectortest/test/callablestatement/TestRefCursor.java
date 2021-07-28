package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.mendix.core.Core;

import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterRefCursor;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class TestRefCursor extends TestCallableStatementBase {

	@Test
	public void testRefCursorOutput() throws Exception {
		StatementBuilder builder = prepareStatementWithCursor(15L);
		executeStatement(builder.getStatement());
		validateRefCursor(15, builder);
	}

	@Test
	public void testRefCursorOutputCallMultipleTimes() throws Exception {
		StatementBuilder builder = prepareStatementWithCursor(15L);
		executeStatement(builder.getStatement());
		executeStatement(builder.getStatement());
		executeStatement(builder.getStatement());
		validateRefCursor(15, builder);
	}

	@Test
	public void testRefCursorOutputEmpty() throws Exception {
		StatementBuilder builder = prepareStatementWithCursor(0L);
		executeStatement(builder.getStatement());
		validateRefCursor(0, builder);
	}

	@Test
	public void testRefCursorByName() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(null, "l_amount", 10L, ParameterLong.class)
				.withRefCursorParameter(null, "result", ParameterMode.OUTPUT)
				.withContent(LONG_TO_REFCURSOR_BY_NAME);
		
		executeStatement(builder.getStatement());
		validateRefCursor(10, builder);
	}

	@Test
	public void testRefCursorNoParameter() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, 10L, ParameterLong.class)
				.withContent(LONG_TO_REFCURSOR);
		
		exceptionRule.expect(SQLException.class);
		exceptionRule.expectMessage("Missing IN or OUT parameter at index");
		executeStatement(builder.getStatement());
	}

	@Test
	public void testRefCursorMultiColumn() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withRefCursorParameter(1, null, ParameterMode.OUTPUT)
				.withContent(REFCURSOR_MULTIPLE_COLUMNS);

		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement());
		assertEquals(3, outputParameters.size());
		
		for (int i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get(i - 1);
			ParameterDecimal longValue = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(1);
			ParameterString stringValue = (ParameterString) currentObject.getParameterObject_Parameter().get(2);
			
			assertEquals(i, longValue.getValue().longValue());
			assertEquals("NAME " + String.valueOf(i), (String) stringValue.getValue());
		}
	}

	@Test
	public void testRefCursorMixedTypes() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withRefCursorParameter(1, null, ParameterMode.OUTPUT)
				.withContent(REFCURSOR_NULL_FIRST);

		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement());
		assertEquals(3, outputParameters.size());
		
		for (int i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get(i - 1);
			ParameterDecimal longValue = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(0);
			if (i < 3L) assertNull(longValue.getValue());
			else assertEquals(i, longValue.getValue().longValue());
		}
	}

	private StatementBuilder prepareStatementWithCursor(Long inputLong) throws Exception {
		return new StatementBuilder(context)
				.withInputParameter(1, null, inputLong, ParameterLong.class)
				.withRefCursorParameter(2, null, ParameterMode.OUTPUT)
				.withContent(LONG_TO_REFCURSOR);
	}
	
	private void validateRefCursor(int expectedCount, StatementBuilder builder) throws Exception {
		
		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement());
		assertEquals(expectedCount, outputParameters.size());
		
		for (int i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get(i - 1);
			ParameterDecimal currentNumber = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(0);
			assertEquals(i, currentNumber.getValue().longValue());
		}
	}

	private List<ParameterObject> getMembersOfCursor(Statement statement) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Statement.MemberNames.Statement_Parameter.toString())
				.stream()
				.filter(p -> p.getType().equals(ParameterRefCursor.entityName))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterRefCursor.MemberNames.ParameterRefCursor_Parameter.toString()).stream())
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
	}
}

package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import com.mendix.core.Core;

import databaseconnector.proxies.Parameter;
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
	public void testRefCursorMultiColumn() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withRefCursorParameter(1, null, ParameterMode.OUTPUT)
				.withContent(REFCURSOR_MULTIPLE_COLUMNS);

		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement(), 1);
		assertEquals(4, outputParameters.size());
		
		for (long i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get((int) i - 1);
			// ParameterString nullValue = (ParameterString) currentObject.getParameterObject_Parameter().get(0);
			ParameterLong longValue = (ParameterLong) currentObject.getParameterObject_Parameter().get(1);
			ParameterString stringValue = (ParameterString) currentObject.getParameterObject_Parameter().get(2);
			
			assertEquals((Long)i, (Long) longValue.getValue());
			assertEquals(String.valueOf(i), (String) stringValue.getValue());
		}
	}

	@Test
	public void testRefCursorMixedTypes() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withRefCursorParameter(1, null, ParameterMode.OUTPUT)
				.withContent(REFCURSOR_NULL_FIRST);

		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement(), 1);
		assertEquals(3, outputParameters.size());
		
		for (long i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get((int) i - 1);
			ParameterLong longValue = (ParameterLong) currentObject.getParameterObject_Parameter().get(0);
			if (i < 3L) assertNull(longValue.getValue());
			else assertEquals((Long)i, (Long) longValue.getValue());
		}
	}

	private StatementBuilder prepareStatementWithCursor(Long inputLong) throws Exception {
		return new StatementBuilder(context)
				.withInputParameter(1, null, inputLong, ParameterLong.class)
				.withRefCursorParameter(2, null, ParameterMode.OUTPUT)
				.withContent(LONG_TO_REFCURSOR);
	}
	
	private void validateRefCursor(int expectedCount, StatementBuilder builder) throws Exception {
		
		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement(), 2);
		assertEquals(expectedCount, outputParameters.size());
		
		for (long i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get((int) i - 1);
			ParameterDecimal currentNumber = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(0);
			assertEquals((Long)(i), (Long) currentNumber.getValue().longValue());
		}
	}

	private List<ParameterObject> getMembersOfCursor(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterRefCursor.MemberNames.ParameterRefCursor_Parameter.toString()).stream())
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
	}
}

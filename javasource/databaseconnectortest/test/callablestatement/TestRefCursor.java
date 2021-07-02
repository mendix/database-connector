package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.LONG_TO_REFCURSOR;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterRefCursor;
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

	private StatementBuilder prepareStatementWithCursor(Long inputLong) throws Exception {
		return new StatementBuilder(context)
				.withInputParameter(1, null, inputLong, ParameterLong.class)
				.withRefCursorParameter(2, null, ParameterMode.OUTPUT)
				.withContent(LONG_TO_REFCURSOR);
	}
	
	private void validateRefCursor(int expectedCount, StatementBuilder builder) throws Exception {
		
		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement(), 2)
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(expectedCount, outputParameters.size());
		
		for (long i = 1; i <= outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get((int) i - 1);
			ParameterDecimal currentNumber = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(0);
			assertEquals((Long)(i), (Long) currentNumber.getValue().longValue());
		}
	}

	private Stream<IMendixObject> getMembersOfCursor(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterRefCursor.MemberNames.ParameterRefCursor_Parameter.toString()).stream());
	}
}

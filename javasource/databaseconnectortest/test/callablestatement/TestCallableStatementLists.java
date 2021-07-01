package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.*;
import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterRefCursor;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class TestCallableStatementLists extends TestCallableStatementBase {

	@Test
	public void testLongListOutput_MultipleValues() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withInputParameter(1, null, 0L, ParameterLong.class)
				.withInputParameter(2, null, 1L, ParameterLong.class)
				.withListOutputParameter(3, null, null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_TWO_LONGS_RETURN_LIST_OF_6);
		
		executeStatement(builder.getStatement());

		List<ParameterDecimal> outputParameters = getMembersOfList(builder.getStatement(), 3)
				.map(p -> ParameterDecimal.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(6, outputParameters.size());
		for (long i = 0; i < outputParameters.size(); i ++) {
			assertEquals((Long)(i % 2), (Long) outputParameters.get((int)i).getValue().longValue());
		}
	}

	@Test
	public void testLongListOutput_Invoke_Twice() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withInputParameter(1, null, 0L, ParameterLong.class)
				.withInputParameter(2, null, 100500L, ParameterLong.class)
				.withListOutputParameter(3, null, null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_TWO_LONGS_RETURN_LIST_OF_6);
		
		executeStatement(builder.getStatement());

		((ParameterLong)(builder.getParameter(1))).setValue(1L);
		executeStatement(builder.getStatement());

		List<ParameterDecimal> outputParameters = getMembersOfList(builder.getStatement(), 3)
				.map(p -> ParameterDecimal.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(6, outputParameters.size());
		for (long i = 0; i < outputParameters.size(); i ++) {
			assertEquals((Long)(i % 2), (Long) outputParameters.get((int)i).getValue().longValue());
		}
	}

	@Test
	public void testDateListOutput_OneValue() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withInputParameter(1, null, TEST_DATE, ParameterDatetime.class)
				.withListOutputParameter(2, null, null, "ARRAY_1_DATE")
				.withContent(TAKE_DATE_RETURN_LIST_OF_1);
		
		executeStatement(builder.getStatement());

		List<ParameterDatetime> outputParameters = getMembersOfList(builder.getStatement(), 2)
				.map(p -> ParameterDatetime.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(1, outputParameters.size());
		assertEquals(TEST_DATE, outputParameters.get(0).getValue());
	}

	@Test
	public void testStringAndDecimalListsOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withInputParameter(1, null, PI, ParameterDecimal.class)
				.withListOutputParameter(2, null, null, "ARRAY_6_NUMBERS")
				.withListOutputParameter(3, null, null, "ARRAY_6_STRINGS")
				.withContent(TAKE_DECIMAL_RETURN_LIST_OF_STRING_AND_LIST_OF_DECIMAL);
		
		executeStatement(builder.getStatement());

		List<ParameterDecimal> decimalList = getMembersOfList(builder.getStatement(), 2)
				.map(p -> ParameterDecimal.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(2, decimalList.size());
		for(ParameterDecimal value : decimalList) { assertEquals(PI.doubleValue(), value.getValue().doubleValue(), 0.15); }

		List<ParameterString> stringList = getMembersOfList(builder.getStatement(), 3)
				.map(p -> ParameterString.initialize(context, p))
				.collect(Collectors.toList());

		assertEquals(3, stringList.size());
		for(ParameterString value : stringList) { assertEquals("test", value.getValue()); }
	}

	@Test
	public void testListOutput_Empty() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_NOTHING_RETURN_EMPTY_LIST);
		
		executeStatement(builder.getStatement());

		Long outputLength = getMembersOfList(builder.getStatement(), 0).count();
		
		assertEquals((Long)0L, outputLength);
	}

	@Test
	public void testInputListOutputLong() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = List.of(builder.longField(1, 1L), builder.longField(2, 2L), builder.longField(3, 4L));
		
		builder = builder
				.withListInputParameter(1, null, inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		executeStatement(builder.getStatement());
		
		assertEquals((Long)7L, ((ParameterLong)builder.getParameter(1)).getValue());
	}

	@Test
	public void testInputEmptyList() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = new LinkedList<ParameterLong>();
		
		builder = builder
				.withListInputParameter(1, null, inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		executeStatement(builder.getStatement());
		
		assertEquals((Long)0L, ((ParameterLong)builder.getParameter(1)).getValue());
	}

	@Test
	public void testInputListSamePosition() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = List.of(builder.longField(1, 1L), builder.longField(1, 2L), builder.longField(3, 4L));
		
		builder = builder
				.withListInputParameter(1, null, inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("Duplicate element at position 1 for list parameter.");
		
		executeStatement(builder.getStatement());
	}

	@Test
	public void testInputListNoPosition() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = List.of(builder.longField(1, 1L), builder.longField(2, 2L), builder.longField(3, 4L));
		
		builder = builder
				.withListInputParameter(null, "test", inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("List parameter used as an input was initialized without a position.");
		
		executeStatement(builder.getStatement());
	}

	@Test
	public void testInputListNoPositionOfElement() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = List.of(builder.longField(1, 1L), builder.longField(null, 2L), builder.longField(3, 4L));
		
		builder = builder
				.withListInputParameter(1, null, inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("Missing position information for element in list.");
		
		executeStatement(builder.getStatement());
	}

	@Test
	public void testListObjectOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);
		
		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_2_OBJ")
				.withContent(ARRAY_2_OBJECTS);
		
		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream())
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(2, outputParameters.size());
	}

	@Test
	public void testListOfListsOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);
		
		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_6_ARRAYS")
				.withContent(ARRAY_6_ARRAYS);
		
		executeStatement(builder.getStatement());

		List<ParameterList> outputParameters = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream())
				.map(p -> ParameterList.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(6, outputParameters.size());
	}

	@Test
	public void testRefCursorOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withInputParameter(1, null, 15L, ParameterLong.class)
				.withRefCursorParameter(2, null, ParameterMode.OUTPUT)
				.withContent(LONG_TO_REFCURSOR);
		
		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = getMembersOfCursor(builder.getStatement(), 2)
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(15, outputParameters.size());
		for (long i = 0; i < outputParameters.size(); i ++) {
			ParameterObject currentObject = outputParameters.get((int) i);
			ParameterDecimal currentNumber = (ParameterDecimal) currentObject.getParameterObject_Parameter().get(0);
			assertEquals((Long)(i), (Long) currentNumber.getValue().longValue());
		}
	}

	private Stream<IMendixObject> getMembersOfList(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream());
	}

	private Stream<IMendixObject> getMembersOfCursor(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterRefCursor.MemberNames.ParameterRefCursor_Parameter.toString()).stream());
	}
}

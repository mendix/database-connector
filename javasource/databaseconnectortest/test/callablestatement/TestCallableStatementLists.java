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
		for (int i = 0; i < outputParameters.size(); i ++) {
			assertEquals(i % 2, outputParameters.get(i).getValue().longValue());
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
		for (int i = 0; i < outputParameters.size(); i ++) {
			assertEquals(i % 2, outputParameters.get((int)i).getValue().longValue());
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
	public void testListOutput_ByName() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withListOutputParameter(null, "result", null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_NOTHING_RETURN_LIST_BY_NAME);

		executeStatement(builder.getStatement());

		List<ParameterDecimal> outputParameters = getMembersOfList(builder.getStatement(), 0)
				.map(p -> ParameterDecimal.initialize(context, p))
				.collect(Collectors.toList());

		assertEquals(3, outputParameters.size());
		for (int i = 0; i < outputParameters.size(); i ++) {
			assertEquals(i, outputParameters.get(i).getValue().longValue());
		}
	}

	@Test
	public void testListOutput_Null() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_NOTHING_RETURN_NULL);

		executeStatement(builder.getStatement());

		long outputLength = getMembersOfList(builder.getStatement(), 0).count();
		
		assertEquals(0, outputLength);
	}

	@Test
	public void testListOutput_Empty() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_6_NUMBERS")
				.withContent(TAKE_NOTHING_RETURN_EMPTY_LIST);
		
		executeStatement(builder.getStatement());

		long outputLength = getMembersOfList(builder.getStatement(), 0).count();
		
		assertEquals(0, outputLength);
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
		
		assertEquals(7, ((ParameterLong)builder.getParameter(1)).getValue().longValue());
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
		
		assertEquals(0, ((ParameterLong)builder.getParameter(1)).getValue().longValue());
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
		exceptionRule.expectMessage("List parameter cannot be used as INPUT or INOUT without a position.");
		
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
		exceptionRule.expectMessage("Parameter was initialized without name or position.");
		
		executeStatement(builder.getStatement());
	}

	@Test
	public void testListObjectOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);
		
		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_2_OBJ")
				.withContent(ARRAY_2_OBJECTS);
		
		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Statement.MemberNames.Statement_Parameter.toString())
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

		List<ParameterList> outputParameters = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Statement.MemberNames.Statement_Parameter.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream())
				.map(p -> ParameterList.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(6, outputParameters.size());
	}


	private Stream<IMendixObject> getMembersOfList(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Statement.MemberNames.Statement_Parameter.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream());
	}
}

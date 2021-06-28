package databaseconnectortest.test.callablestatement;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.impl.JdbcConnector;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;
import databaseconnectortest.proxies.constants.Constants;

import static databaseconnectortest.test.callablestatement.Queries.*;

public class TestCallableStatement {
	private final IContext context = Core.createSystemContext();
	
	private final static Long AU = 149597870700L;
	private final static BigDecimal PI = new BigDecimal(3.14);
	private final static String TEST_STRING = "Nibiru cataclysm";
	private final static Date TEST_DATE = new Date(0L);
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@BeforeClass
	public static void prepare() throws Exception {
		executeStatement(CREATE_TYPE_NAME_AND_AGE);
		executeStatement(CREATE_TYPE_ARRAY_6_NUMBERS);
		executeStatement(CREATE_TYPE_ARRAY_1_DATE);
		executeStatement(CREATE_TYPE_ARRAY_6_STRINGS);
		executeStatement(CREATE_TYPE_ARRAY_6_STRINGS);
		executeStatement(CREATE_PROCEDURE_LONG_TO_LONG);
		executeStatement(CREATE_PROCEDURE_LONG_TO_DIFFERENT_LONG);
		executeStatement(CREATE_PROCEDURE_OBJECT_TO_OBJECT);
	}
	
	@Test
	public void testAllArgumentTypes() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, AU, ParameterLong.class)
				.withInputParameter(2, null, PI, ParameterDecimal.class)
				.withInputParameter(3, null, TEST_STRING, ParameterString.class)
				.withInputParameter(4, null, TEST_DATE, ParameterDatetime.class)
				.withOutputParameter(5, null, ParameterDatetime.class)
				.withOutputParameter(6, null, ParameterString.class)
				.withOutputParameter(7, null, ParameterDecimal.class)
				.withOutputParameter(8, null, ParameterLong.class)
				.withContent(TAKE_ALL_TYPES_RETURN_ALL_TYPES);
		
		executeStatement(builder.getStatement());

		assertEquals(TEST_DATE, ((ParameterDatetime)builder.getParameter(4)).getValue());
		assertEquals(TEST_STRING, ((ParameterString)builder.getParameter(5)).getValue());
		assertEquals(PI.doubleValue(), ((ParameterDecimal)builder.getParameter(6)).getValue().doubleValue(), 1e-10);
		assertEquals(AU, ((ParameterLong)builder.getParameter(7)).getValue());
	}

	@Test
	public void testOneArgumentByPosition() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, AU, ParameterLong.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent(TAKE_LONG_RETURN_CHAR);
		
		executeStatement(builder.getStatement());

		assertEquals(AU.toString(), ((ParameterString)builder.getParameter(1)).getValue());
	}
	
	@Test
	public void testOneArgumentByName() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(null, "in_long", BigDecimal.valueOf(AU), ParameterDecimal.class)
				.withOutputParameter(null, "result", ParameterString.class)
				.withContent(INOUT_ARGUMENT_BY_NAME);
		
		executeStatement(builder.getStatement());
		
		assertEquals(((Long)(AU * 3)).toString(), ((ParameterString)builder.getParameter(1)).getValue());
	}
	
	@Test
	public void testOneArgumentByPositionInProcedure() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, BigDecimal.valueOf(AU), ParameterDecimal.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent("{ call long_to_different_long(:1, :2) }");
		
		executeStatement(builder.getStatement());
		
		assertEquals(((Long)(AU * 3)).toString(), ((ParameterString)builder.getParameter(1)).getValue());
	}
	
	@Test
	public void testInOutArgumentByPosition() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInOutParameter(1, null, AU, ParameterLong.class)
				.withContent("{ call long_to_long(:1) }");
		
		executeStatement(builder.getStatement());

		assertEquals((Long)(AU * 2), ((ParameterLong)builder.getParameter(0)).getValue());
	}
	
	@Test
	public void testInOutArgumentByName() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInOutParameter(null, "param", AU, ParameterLong.class)
				.withContent("{ call long_to_long(:param) }");
		
		executeStatement(builder.getStatement());

		assertEquals((Long)(AU * 2), ((ParameterLong)builder.getParameter(0)).getValue());
	}
	
	@Test
	public void executeTwice() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, AU * 2, ParameterLong.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent(TAKE_LONG_RETURN_CHAR);
		
		executeStatement(builder.getStatement());
		((ParameterLong)builder.getParameter(0)).setValue(AU);
		executeStatement(builder.getStatement());

		assertEquals(AU.toString(), ((ParameterString)builder.getParameter(1)).getValue());
	}

	@Test
	public void possibleException() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, 1L, ParameterLong.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent(DIVIDE_BY_INPUT);
		
		executeStatement(builder.getStatement());
		
		((ParameterLong)builder.getParameter(0)).setValue(0L);
		
		exceptionRule.expect(SQLDataException.class);
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void wrongInputParameterIndex() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(0, null, 1L, ParameterLong.class)
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LONG_RETURN_LONG);
		
		exceptionRule.expect(SQLException.class);
		exceptionRule.expectMessage("Invalid column index");
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void outputLongToString() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, AU, ParameterLong.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent(TAKE_LONG_RETURN_LONG);
		
		executeStatement(builder.getStatement());
		
		assertEquals(AU.toString(), ((ParameterString)builder.getParameter(1)).getValue());
	}

	@Test
	public void missingInputParameter() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LONG_RETURN_LONG);

		exceptionRule.expect(SQLException.class);
		exceptionRule.expectMessage("Missing IN or OUT parameter");
		executeStatement(builder.getStatement());
	}

	@Test
	public void missingOutputParameter() throws Exception {		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, AU, ParameterLong.class)
				.withContent(TAKE_LONG_RETURN_LONG);

		exceptionRule.expect(SQLException.class);
		exceptionRule.expectMessage("Missing IN or OUT parameter");
		executeStatement(builder.getStatement());
	}

	@Test
	public void wrongInputParameterMode() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, TEST_STRING, ParameterString.class)
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LONG_RETURN_LONG);

		exceptionRule.expect(SQLException.class);
		executeStatement(builder.getStatement());
	}

	@Test
	public void testObjectInput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.stringField(1, TEST_STRING));
		inputObjectFields.add(builder.longField(2, AU));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);
		
		executeStatement(builder.getStatement());

		assertEquals(TEST_STRING, ((ParameterString)builder.getParameter(1)).getValue());
		assertEquals(AU, ((ParameterLong)builder.getParameter(2)).getValue());
	}
	
	@Test
	public void testObjectInputTooManyMembers() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.stringField(1, TEST_STRING));
		inputObjectFields.add(builder.longField(2, AU));
		inputObjectFields.add(builder.longField(3, AU));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);
		
		exceptionRule.expect(SQLException.class);
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void testObjectInputTooFewMembers() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.stringField(1, TEST_STRING));

		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);

		exceptionRule.expect(SQLException.class);
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void testObjectInputMixedUpTypes() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.longField(1, AU));
		inputObjectFields.add(builder.stringField(2, TEST_STRING));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);
		
		exceptionRule.expect(NumberFormatException.class);
		exceptionRule.expectMessage("For input string");
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void testObjectInputDuplicatePosition() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.longField(2, AU));
		inputObjectFields.add(builder.stringField(2, TEST_STRING));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);

		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("Duplicate field position in object parameter at position 2.");
		executeStatement(builder.getStatement());
	}

	@Test
	public void testObjectOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.stringField(1, null));
		outputObjectFields.add(builder.longField(2, null));
		
		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		executeStatement(builder.getStatement());

		long numberOfOutputFields = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterObject.MemberNames.ParameterObject_Parameter.toString()).stream()).count();

		assertEquals(outputObjectFields.size(), numberOfOutputFields);

		assertEquals(TEST_STRING, ((ParameterString) outputObjectFields.get(0)).getValue());
		assertEquals(AU, ((ParameterLong) outputObjectFields.get(1)).getValue());
	}

	@Test
	public void testObjectOutputTooFewParameters() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.stringField(1, null));
		
		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		exceptionRule.expect(DatabaseConnectorException.class);
		exceptionRule.expectMessage("Number of values for object do not match number of expected fields. Expected 1, retrieved 2.");
		executeStatement(builder.getStatement());

	}

	@Test
	public void testObjectOutputDuplicateKeys() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.stringField(2, null));
		outputObjectFields.add(builder.longField(2, null));
		
		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("Duplicate field position in object parameter at position 2.");
		executeStatement(builder.getStatement());
	}

	@Test
	public void testObjectOutputTooManyParameters() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.stringField(1, null));
		outputObjectFields.add(builder.longField(2, null));
		outputObjectFields.add(builder.longField(3, null));

		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		exceptionRule.expect(DatabaseConnectorException.class);
		exceptionRule.expectMessage("Number of values for object do not match number of expected fields. Expected 3, retrieved 2.");
		executeStatement(builder.getStatement());
	}

	@Test
	public void testObjectOutputMixedUpParameters() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.longField(1, null));
		outputObjectFields.add(builder.stringField(2, null));

		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		exceptionRule.expect(DatabaseConnectorException.class);
		exceptionRule.expectMessage("Unable to set field of ParameterObject");
		executeStatement(builder.getStatement());
	}

	@Test
	public void multipleObjects() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputFields4 = objectFields(null, null, builder);
		List<Parameter> outputFields5 = objectFields(null, null, builder);
		List<Parameter> outputFields6 = objectFields(null, null, builder);
		
		builder = builder
				.withObjectInputParameter(1, null, objectFields("A", 1L, builder), "NAME_AND_AGE")
				.withObjectInputParameter(2, null, objectFields("B", 2L, builder), "NAME_AND_AGE")
				.withObjectInputParameter(3, null, objectFields("C", 3L, builder), "NAME_AND_AGE")
				.withObjectOutputParameter(4, null, outputFields4, "NAME_AND_AGE")
				.withObjectOutputParameter(5, null, outputFields5, "NAME_AND_AGE")
				.withObjectOutputParameter(6, null, outputFields6, "NAME_AND_AGE")
				.withContent(TAKE_OBJECTS_RETURN_OBJECTS);

		executeStatement(builder.getStatement());

		assertEquals("C", ((ParameterString) outputFields4.get(0)).getValue());
		assertEquals((Long)3L, ((ParameterLong) outputFields4.get(1)).getValue());
		assertEquals("B", ((ParameterString) outputFields5.get(0)).getValue());
		assertEquals((Long)2L, ((ParameterLong) outputFields5.get(1)).getValue());
		assertEquals("A", ((ParameterString) outputFields6.get(0)).getValue());
		assertEquals((Long)1L, ((ParameterLong) outputFields6.get(1)).getValue());
	}

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

	@Ignore
	@Test
	public void testInputListOfLongsOutputLong() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<ParameterLong> inputList = List.of(builder.longField(1, 1L), builder.longField(2, 2L), builder.longField(3, 4L));
		
		builder = builder
				.withListInputParameter(1, null, inputList, "ARRAY_6_NUMBERS")
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LIST_OF_LONG_RETURN_SUM);
		
		executeStatement(builder.getStatement());
		
		assertEquals((Long)7L, ((ParameterLong)builder.getParameter(1)).getValue());
	}

	@Ignore
	@Test
	public void testListObjectOutput() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);
		
		builder = builder
				.withListOutputParameter(1, null, null, "ARRAY_2_OBJ")
				.withContent(
						"declare\r\n" + 
						"begin\r\n" +
						"  :1 := array_number(NAME_AND_AGE('a', 1), NAME_AND_AGE('b', 2);\r\n" +
						"end;");
		
		executeStatement(builder.getStatement());

		List<ParameterObject> outputParameters = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterObject.MemberNames.ParameterObject_Parameter.toString()).stream())
				.map(p -> ParameterObject.initialize(context, p))
				.collect(Collectors.toList());
		
		assertEquals(2, outputParameters.size());
	}

	@Test
	public void inOutObject() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> fields = objectFields(TEST_STRING, AU, builder);
		
		builder = builder
				.withObjectInOutParameter(1, null, fields, "NAME_AND_AGE")
				.withContent("{ call object_to_same_object(:1) }");

		executeStatement(builder.getStatement());

		assertEquals("new value", ((ParameterString) fields.get(0)).getValue());
		assertEquals((Long)(AU * 2), ((ParameterLong) fields.get(1)).getValue());
	}

	private Stream<IMendixObject> getMembersOfList(Statement statement, int position) {
		return Core.retrieveByPath(context, statement.getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.Position.toString()).equals(position))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterList.MemberNames.ParameterList_Parameter.toString()).stream());
	}
	
	private List<Parameter> objectFields(String name, Long age, StatementBuilder builder) throws Exception {
		List<Parameter> objectFields = new LinkedList<Parameter>();
		objectFields.add(builder.stringField(1, name));
		objectFields.add(builder.longField(2, age));
		return objectFields;
	}
	
	private StatementBuilder builderFromObjectToMembers(StatementBuilder builder, List<Parameter> inputObjectFields) throws Exception {
		return builder
				.withObjectInputParameter(1, null, inputObjectFields, "NAME_AND_AGE")
				.withOutputParameter(2, null, ParameterString.class)
				.withOutputParameter(3, null, ParameterLong.class);
	}
	
	private StatementBuilder builderFromMembersToObject(StatementBuilder builder, List<Parameter> outputObjectFields) throws Exception {
		return builder
				.withInputParameter(1, null, TEST_STRING, ParameterString.class)
				.withInputParameter(2, null, AU, ParameterLong.class)
				.withObjectOutputParameter(3, null, outputObjectFields, "NAME_AND_AGE");
	}
	
	private static void executeStatement(Statement statement) throws SQLException, DatabaseConnectorException {
		ILogNode logNode = Core.getLogger("DatabaseConnectorTest");
		new JdbcConnector(logNode).executeCallableStatement("jdbc:oracle:thin:@//" + Constants.getOracleAddress(), Constants.getOracleUserName(), Constants.getOraclePassword(), statement);
	}

	private static void executeStatement(String content) throws SQLException, DatabaseConnectorException {
		executeStatement(new StatementBuilder(Core.createSystemContext()).withContent(content).getStatement());
	}
}

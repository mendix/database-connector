package databaseconnectortest.test.callablestatement;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.impl.JdbcConnector;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;
import databaseconnectortest.proxies.constants.Constants;

public class TestCallableStatement {
	private final ILogNode logNode = Core.getLogger("DatabaseConnectorTest");
	private final IContext context = Core.createSystemContext();
	
	private final static Long AU = 149597870700L;
	private final static String TEST_STRING = "Nibiru cataclysm";
	
	private final static String TAKE_LONG_RETURN_LONG =
			"declare\r\n" + 
			"  l_long number(20,0) := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := l_long;\r\n" + 
			"end;";

	private final static String TAKE_LONG_RETURN_CHAR =
			"declare\r\n" + 
			"  l_long number(20,0) := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := TO_CHAR(l_long);\r\n" + 
			"end;";

	private final static String TAKE_OBJECT_RETURN_MEMBERS =
			"declare\r\n" +
			"  l_rec NAME_AND_AGE := :1;\r\n" + 
			"begin\r\n" + 
			"  :2 := l_rec.name;\r\n" + 
			"  :3 := l_rec.age;\r\n" + 
			"end;";

	private final static String TAKE_MEMBERS_RETURN_OBJECT =
			"declare\r\n" +
			"  name VARCHAR2(100) := :1;\r\n" + 
			"  age NUMBER := :2;\r\n" + 
			"begin\r\n" + 
			"  :3 := NAME_AND_AGE(name, age);\r\n" + 
			"end;";

	private final static String TAKE_OBJECTS_RETURN_OBJECTS =
			"declare\r\n" +
			"  input1 NAME_AND_AGE := :1;\r\n" + 
			"  input2 NAME_AND_AGE := :2;\r\n" + 
			"  input3 NAME_AND_AGE := :3;\r\n" + 
			"begin\r\n" + 
			"  :4 := input3;\r\n" + 
			"  :5 := input2;\r\n" + 
			"  :6 := input1;\r\n" + 
			"end;";

	private final JdbcConnector connector = new JdbcConnector(logNode);
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Before
	public void prepare() throws Exception {
		executeStatement(new StatementBuilder(context)
				.withContent(
					"CREATE OR REPLACE PROCEDURE long_to_long (lval IN OUT NUMBER) AS\r\n" + 
					"   BEGIN\r\n" + 
					"   lval := lval * 2;\r\n" + 
					" END;")
				.getStatement());
		
		executeStatement(new StatementBuilder(context)
				.withContent(
					"create or replace type name_and_age is object (\r\n" +
					"    name VARCHAR2(100), age NUMBER\r\n" +
					")")
				.getStatement());
		
		executeStatement(new StatementBuilder(context)
				.withContent(
					"CREATE OR REPLACE PROCEDURE object_to_same_object (lval IN OUT NAME_AND_AGE) AS\r\n" + 
					"   BEGIN\r\n" + 
					"   lval.age := lval.age * 2;\r\n" + 
					"   lval.name := 'new value';\r\n" + 
					" END;")
				.getStatement());
	}
	
	@Test
	public void testCallableStatementPrimitives() throws Exception {
		// Arrange
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, new Date(0L), ParameterDatetime.class)
				.withInputParameter(2, null, BigDecimal.valueOf(AU), ParameterDecimal.class)
				.withOutputParameter(3, null, ParameterLong.class)
				.withOutputParameter(4, null, ParameterString.class)
				.withContentFromFile("tests\\callableStatementPrimitivesOracle.sql");
					
		// Act
		executeStatement(builder.getStatement());
		
		// Assert
		assertEquals(AU, ((ParameterLong)builder.getParameter(2)).getValue());
		assertEquals("1970-01-01 01:01:00", ((ParameterString)builder.getParameter(3)).getValue());
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
		String content =
				"declare\r\n" + 
				"  l_long number(20,0) := :in_long;\r\n" + 
				"  result out varchar2;\r\n" + 
				"begin\r\n" + 
				"  :result := TO_CHAR(l_long);\r\n" + 
				"end;";
		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(null, "in_long", BigDecimal.valueOf(AU), ParameterDecimal.class)
				.withOutputParameter(null, "result", ParameterString.class)
				.withContent(content);
		
		executeStatement(builder.getStatement());

		assertEquals(AU.toString(), ((ParameterString)builder.getParameter(1)).getValue());
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
		String content =
				"declare\r\n" + 
				"  l_long number(20,0) := :1;\r\n" + 
				"begin\r\n" + 
				"  :2 := 1 / l_long;\r\n" + 
				"end;";
		
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, 1L, ParameterLong.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent(content);
		
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
		inputObjectFields.add(builder.initStringParameter(1, null, TEST_STRING, ParameterMode.INPUT));
		inputObjectFields.add(builder.initLongParameter(2, null, AU, ParameterMode.INPUT));
		
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
		inputObjectFields.add(builder.initStringParameter(1, null, TEST_STRING, ParameterMode.INPUT));
		inputObjectFields.add(builder.initLongParameter(2, null, AU, ParameterMode.INPUT));
		inputObjectFields.add(builder.initLongParameter(3, null, AU, ParameterMode.INPUT));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);
		
		exceptionRule.expect(SQLException.class);
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void testObjectInputTooFewMembers() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.initStringParameter(1, null, TEST_STRING, ParameterMode.INPUT));

		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);

		exceptionRule.expect(SQLException.class);
		executeStatement(builder.getStatement());
	}
	
	@Test
	public void testObjectInputMixedUpTypes() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.initLongParameter(1, null, AU, ParameterMode.INPUT));
		inputObjectFields.add(builder.initStringParameter(2, null, TEST_STRING, ParameterMode.INPUT));
		
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
		inputObjectFields.add(builder.initLongParameter(2, null, AU, ParameterMode.INPUT));
		inputObjectFields.add(builder.initStringParameter(2, null, TEST_STRING, ParameterMode.INPUT));
		
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
		outputObjectFields.add(builder.initStringParameter(1, null, null, ParameterMode.OUTPUT));
		outputObjectFields.add(builder.initLongParameter(2, null, null, ParameterMode.OUTPUT));
		
		builder = builderFromMembersToObject(builder, outputObjectFields)
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		executeStatement(builder.getStatement());

		long numberOfOutputFields = Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Parameter.MemberNames.Parameter_Statement.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, Parameter.MemberNames.MemberOfObject.toString(), true).stream()).count();

		assertEquals(outputObjectFields.size(), numberOfOutputFields);

		assertEquals(TEST_STRING, ((ParameterString) outputObjectFields.get(0)).getValue());
		assertEquals(AU, ((ParameterLong) outputObjectFields.get(1)).getValue());
	}

	@Test
	public void testObjectOutputTooFewParameters() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.initStringParameter(1, null, null, ParameterMode.OUTPUT));
		
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
		outputObjectFields.add(builder.initStringParameter(2, null, null, ParameterMode.OUTPUT));
		outputObjectFields.add(builder.initLongParameter(2, null, null, ParameterMode.OUTPUT));
		
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
		outputObjectFields.add(builder.initStringParameter(1, null, null, ParameterMode.OUTPUT));
		outputObjectFields.add(builder.initLongParameter(2, null, null, ParameterMode.OUTPUT));
		outputObjectFields.add(builder.initLongParameter(3, null, null, ParameterMode.OUTPUT));

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
		outputObjectFields.add(builder.initLongParameter(1, null, null, ParameterMode.OUTPUT));
		outputObjectFields.add(builder.initStringParameter(2, null, null, ParameterMode.OUTPUT));

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


	@Test
	public void testAllArgumentTypes() throws Exception {
		// TODO
	}
	
	private List<Parameter> objectFields(String name, Long age, StatementBuilder builder) throws Exception {
		List<Parameter> objectFields = new LinkedList<Parameter>();
		objectFields.add(builder.initStringParameter(1, null, name, ParameterMode.INPUT));
		objectFields.add(builder.initLongParameter(2, null, age, ParameterMode.INPUT));
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
	
	private void executeStatement(Statement statement) throws SQLException, DatabaseConnectorException {
		connector.executeCallableStatement("jdbc:oracle:thin:@//" + Constants.getOracleAddress(), Constants.getOracleUserName(), Constants.getOraclePassword(), statement);
	}
}

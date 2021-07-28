package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.TAKE_MEMBERS_RETURN_OBJECT;
import static databaseconnectortest.test.callablestatement.Queries.TAKE_OBJECTS_RETURN_OBJECTS;
import static databaseconnectortest.test.callablestatement.Queries.TAKE_OBJECT_RETURN_MEMBERS;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import com.mendix.core.Core;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class TestCallableStatementObjects extends TestCallableStatementBase {
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
	public void testObjectInputNulls() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> inputObjectFields = new LinkedList<Parameter>();
		inputObjectFields.add(builder.stringField(1, null));
		inputObjectFields.add(builder.longField(2, null));
		
		builder = builderFromObjectToMembers(builder, inputObjectFields)
				.withContent(TAKE_OBJECT_RETURN_MEMBERS);
		
		executeStatement(builder.getStatement());

		assertEquals(null, ((ParameterString)builder.getParameter(1)).getValue());
		assertEquals(null, ((ParameterLong)builder.getParameter(2)).getValue());
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

		assertEquals(outputObjectFields.size(), countFields(builder));

		assertEquals(TEST_STRING, ((ParameterString) outputObjectFields.get(0)).getValue());
		assertEquals(AU, ((ParameterLong) outputObjectFields.get(1)).getValue());
	}

	@Test
	public void testObjectOutputNulls() throws Exception {
		StatementBuilder builder = new StatementBuilder(context);

		List<Parameter> outputObjectFields = new LinkedList<Parameter>();
		outputObjectFields.add(builder.stringField(1, null));
		outputObjectFields.add(builder.longField(2, null));
		
		builder = builder
				.withInputParameter(1, null, null, ParameterString.class)
				.withInputParameter(2, null, null, ParameterLong.class)
				.withObjectOutputParameter(3, null, outputObjectFields, "NAME_AND_AGE")
				.withContent(TAKE_MEMBERS_RETURN_OBJECT);

		executeStatement(builder.getStatement());

		assertEquals(outputObjectFields.size(), countFields(builder));

		assertEquals(null, ((ParameterString) outputObjectFields.get(0)).getValue());
		assertEquals(null, ((ParameterLong) outputObjectFields.get(1)).getValue());
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
		exceptionRule.expectMessage("Unable to set value Nibiru cataclysm for parameter DatabaseConnector.ParameterLong.");
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
		assertEquals(3, ((ParameterLong) outputFields4.get(1)).getValue().longValue());
		assertEquals("B", ((ParameterString) outputFields5.get(0)).getValue());
		assertEquals(2, ((ParameterLong) outputFields5.get(1)).getValue().longValue());
		assertEquals("A", ((ParameterString) outputFields6.get(0)).getValue());
		assertEquals(1, ((ParameterLong) outputFields6.get(1)).getValue().longValue());
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
		assertEquals(AU * 2, ((ParameterLong) fields.get(1)).getValue().longValue());
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

	private long countFields(StatementBuilder builder) throws Exception {
		return Core.retrieveByPath(context, builder.getStatement().getMendixObject(), Statement.MemberNames.Statement_Parameter.toString())
				.stream()
				.filter(p -> p.getValue(context, Parameter.MemberNames.ParameterMode.toString()).equals(ParameterMode.OUTPUT.toString()))
				.flatMap(p -> Core.retrieveByPath(context, p, ParameterObject.MemberNames.ParameterObject_Parameter.toString()).stream()).count();
	}
}

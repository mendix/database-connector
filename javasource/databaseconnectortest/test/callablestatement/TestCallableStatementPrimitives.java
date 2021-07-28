package databaseconnectortest.test.callablestatement;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import org.junit.Test;

import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterString;

import static databaseconnectortest.test.callablestatement.Queries.*;

public class TestCallableStatementPrimitives extends TestCallableStatementBase {
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
	public void testAllNulls() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, null, ParameterLong.class)
				.withInputParameter(2, null, null, ParameterDecimal.class)
				.withInputParameter(3, null, null, ParameterString.class)
				.withInputParameter(4, null, null, ParameterDatetime.class)
				.withOutputParameter(5, null, ParameterDatetime.class)
				.withOutputParameter(6, null, ParameterString.class)
				.withOutputParameter(7, null, ParameterDecimal.class)
				.withOutputParameter(8, null, ParameterLong.class)
				.withContent(TAKE_ALL_TYPES_RETURN_ALL_TYPES);
		
		executeStatement(builder.getStatement());

		assertEquals(null, ((ParameterDatetime)builder.getParameter(4)).getValue());
		assertEquals(null, ((ParameterString)builder.getParameter(5)).getValue());
		assertEquals(null, ((ParameterDecimal)builder.getParameter(6)).getValue());
		assertEquals(null, ((ParameterLong)builder.getParameter(7)).getValue());
	}

	@Test
	public void testDecimalToLong() throws Exception {
		
		StatementBuilder builder = new StatementBuilder(context)
				.withOutputParameter(1, null, ParameterLong.class)
				.withOutputParameter(2, null, ParameterLong.class)
				.withOutputParameter(3, null, ParameterLong.class)
				.withOutputParameter(4, null, ParameterLong.class)
				.withContent(RETURN_DECIMAL);

		executeStatement(builder.getStatement());

		assertEquals(FIVE, ((ParameterLong)builder.getParameter(0)).getValue());
		assertEquals(FIVE, ((ParameterLong)builder.getParameter(1)).getValue());
		assertEquals(FIVE, ((ParameterLong)builder.getParameter(2)).getValue());
		assertEquals(FIVE, ((ParameterLong)builder.getParameter(3)).getValue());
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
		
		assertEquals(String.valueOf(AU * 3), ((ParameterString)builder.getParameter(1)).getValue());
	}
	
	@Test
	public void testOneArgumentByPositionInProcedure() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, BigDecimal.valueOf(AU), ParameterDecimal.class)
				.withOutputParameter(2, null, ParameterString.class)
				.withContent("{ call long_to_different_long(:1, :2) }");
		
		executeStatement(builder.getStatement());
		
		assertEquals(String.valueOf(AU * 3), ((ParameterString)builder.getParameter(1)).getValue());
	}

	@Test
	public void testInOutArgumentByPosition() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInOutParameter(1, null, AU, ParameterLong.class)
				.withContent("{ call long_to_long(:1) }");
		
		executeStatement(builder.getStatement());

		assertEquals(AU * 2, ((ParameterLong)builder.getParameter(0)).getValue().longValue());
	}
	
	@Test
	public void testInOutArgumentByName() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInOutParameter(null, "param", AU, ParameterLong.class)
				.withContent("{ call long_to_long(:param) }");
		
		executeStatement(builder.getStatement());

		assertEquals(AU * 2, ((ParameterLong)builder.getParameter(0)).getValue().longValue());
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
		
		exceptionRule.expect(IllegalArgumentException.class);
		exceptionRule.expectMessage("Parameter was initialized without name or position.");
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
}

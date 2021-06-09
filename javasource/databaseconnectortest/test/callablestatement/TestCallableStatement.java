package databaseconnectortest.test.callablestatement;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Date;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;

import databaseconnector.impl.JdbcConnector;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
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
	    exceptionRule.expectMessage("ORA-01476");
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
	public void wrongInputParameterType() throws Exception {
		StatementBuilder builder = new StatementBuilder(context)
				.withInputParameter(1, null, TEST_STRING, ParameterString.class)
				.withOutputParameter(2, null, ParameterLong.class)
				.withContent(TAKE_LONG_RETURN_LONG);

	    exceptionRule.expect(SQLException.class);
	    exceptionRule.expectMessage("ORA-06502");
		executeStatement(builder.getStatement());
	}


	@Test
	public void testAllArgumentTypes() throws Exception {
		// TODO
	}
	
	
	
	private void executeStatement(Statement statement) throws SQLException {
		connector.executeCallableStatement("jdbc:oracle:thin:@//" + Constants.getOracleAddress(), Constants.getOracleUserName(), Constants.getOraclePassword(), statement);
	}
}

package databaseconnectortest.test.callablestatement;

import static databaseconnectortest.test.callablestatement.Queries.*;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.impl.JdbcConnector;
import databaseconnector.proxies.Statement;
import databaseconnectortest.proxies.constants.Constants;

public class TestCallableStatementBase {
	protected final IContext context = Core.createSystemContext();
	
	protected final static Long AU = 149597870700L;
	protected final static BigDecimal PI = new BigDecimal(3.14);
	protected final static String TEST_STRING = "Nibiru cataclysm";
	protected final static Date TEST_DATE = new Date(0L);
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@BeforeClass
	public static void prepare() throws Exception {
		dropType("ARRAY_2_OBJ");
		dropType("ARRAY_6_ARRAYS");

		executeStatement(CREATE_TYPE_NAME_AND_AGE);
		executeStatement(CREATE_TYPE_ARRAY_6_NUMBERS);
		executeStatement(CREATE_TYPE_ARRAY_1_DATE);
		executeStatement(CREATE_TYPE_ARRAY_6_STRINGS);
		executeStatement(CREATE_TYPE_ARRAY_2_OBJ);
		executeStatement(CREATE_PROCEDURE_LONG_TO_LONG);
		executeStatement(CREATE_PROCEDURE_LONG_TO_DIFFERENT_LONG);
		executeStatement(CREATE_PROCEDURE_OBJECT_TO_OBJECT);
		executeStatement(CREATE_TYPE_ARRAY_6_ARRAYS);
	}

	protected static void executeStatement(Statement statement) throws SQLException, DatabaseConnectorException {
		ILogNode logNode = Core.getLogger("DatabaseConnectorTest");
		new JdbcConnector(logNode).executeCallableStatement("jdbc:oracle:thin:@//" + Constants.getOracleAddress(), Constants.getOracleUserName(), Constants.getOraclePassword(), statement);
	}

	protected static void executeStatement(String content) throws SQLException, DatabaseConnectorException {
		executeStatement(new StatementBuilder(Core.createSystemContext()).withContent(content).getStatement());
	}
	
	protected static void dropType(String typeName) throws SQLException, DatabaseConnectorException {
		executeStatement(
				"begin\r\n" + 
				"   execute immediate 'DROP TYPE " + typeName + "';\r\n" + 
				"exception when others then\r\n" + 
				"   if sqlcode != -4043 then\r\n" + 
				"      raise;\r\n" + 
				"   end if;\r\n" + 
				"end;");
	}
}

package databaseconnectortest.test.callablestatement;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Date;

import org.junit.Test;

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

	private final JdbcConnector connector = new JdbcConnector(logNode);

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
		assertEquals(((ParameterLong)builder.getParameter(2)).getValue(), AU);
		assertEquals(((ParameterString)builder.getParameter(3)).getValue(), "1970-01-01 01:01:00");
	}
	
	private void executeStatement(Statement statement) throws SQLException {
		connector.executeCallableStatement("jdbc:oracle:thin:@//" + Constants.getOracleAddress(), Constants.getOracleUserName(), Constants.getOraclePassword(), statement);
	}
}

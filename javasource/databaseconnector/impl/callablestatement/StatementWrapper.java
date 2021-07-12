package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.List;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.ParameterMode;

/**
 * Class that wraps CallableStatements and maintains the list of the objects
 * containing its input and output parameters.
 * 
 * Provides an execute parameter that ensures output parameters are updated
 * after the statement is executed.
 * 
 * Also implements AutoCloseable for convenience.
 */
public class StatementWrapper implements AutoCloseable {
	final private CallableStatement cStatement;
	final private List<SqlParameter> parameters;

	public StatementWrapper(final CallableStatement cStatement, final List<SqlParameter> parameters) {
		this.cStatement = cStatement;
		this.parameters = parameters;
	}

	public void execute() throws SQLException, DatabaseConnectorException {
		this.cStatement.execute();

		for (SqlParameter p : this.parameters) {
			if (p.getParameterMode().equals(ParameterMode.OUTPUT) || p.getParameterMode().equals(ParameterMode.INOUT)) {
				p.retrieveResult(cStatement);
			}
		}
	}

	@Override
	public void close() throws SQLException {
		this.cStatement.close();
	}
}

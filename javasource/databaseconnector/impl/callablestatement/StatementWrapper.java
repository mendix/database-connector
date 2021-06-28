package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.List;

import databaseconnector.impl.DatabaseConnectorException;

public class StatementWrapper implements AutoCloseable {
	final private CallableStatement cStatement;
	final private List<SqlParameter<?>> parameters;

	public StatementWrapper(final CallableStatement cStatement, final List<SqlParameter<?>> parameters) {
		this.cStatement = cStatement;
		this.parameters = parameters;
	}

	public void execute() throws SQLException, DatabaseConnectorException {
		this.cStatement.execute();

		for (SqlParameter<?> p : this.parameters) {
			p.retrieveResult(cStatement);
		}
	}

	@Override
	public void close() throws SQLException {
		this.cStatement.close();
	}
}

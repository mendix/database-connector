package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import databaseconnector.impl.DatabaseConnectorException;

public interface SqlParameter {
	void prepareCall(CallableStatement cStatement) throws SQLException;

	void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;
}

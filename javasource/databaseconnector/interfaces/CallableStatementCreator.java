package databaseconnector.interfaces;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.impl.callablestatement.StatementWrapper;
import databaseconnector.proxies.Statement;

import java.sql.Connection;
import java.sql.SQLException;

public interface CallableStatementCreator {
	StatementWrapper create(final Statement statement, final Connection connection) throws SQLException, DatabaseConnectorException;
}

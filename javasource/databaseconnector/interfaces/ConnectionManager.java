package databaseconnector.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionManager {
	Connection getConnection(final String jdbcUrl, final String userName, final String password) throws SQLException;
}

package databaseconnector.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionManager {
  Connection getConnection(String jdbcUrl, String userName, String password) throws SQLException;
}

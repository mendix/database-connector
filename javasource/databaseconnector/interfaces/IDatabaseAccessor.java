package databaseconnector.interfaces;

import java.sql.Connection;
import java.sql.SQLException;

public interface IDatabaseAccessor {
  Connection getConnection() throws SQLException;
}
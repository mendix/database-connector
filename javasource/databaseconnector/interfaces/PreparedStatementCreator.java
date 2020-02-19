package databaseconnector.interfaces;

import com.mendix.systemwideinterfaces.javaactions.parameters.IStringTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface PreparedStatementCreator {
    PreparedStatement create(String query, Connection connection) throws SQLException;

    PreparedStatement create(IStringTemplate sql, Connection connection) throws SQLException, IllegalArgumentException;
}

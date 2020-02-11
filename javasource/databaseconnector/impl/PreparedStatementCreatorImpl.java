package databaseconnector.impl;

import com.mendix.systemwideinterfaces.javaactions.parameters.IStringTemplate;
import com.mendix.systemwideinterfaces.javaactions.parameters.ITemplateParameter;
import databaseconnector.interfaces.PreparedStatementCreator;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class PreparedStatementCreatorImpl implements PreparedStatementCreator {

    @Override
    public PreparedStatement create(String query, Connection connection) throws SQLException {
        return connection.prepareStatement(query);
    }

    @Override
    public PreparedStatement create(IStringTemplate sql, Connection connection) throws SQLException {
        List<ITemplateParameter> originalParameters = sql.getParameters();
        List<ITemplateParameter> queryParameters = new ArrayList<ITemplateParameter>();

        String queryTemplate = sql.replacePlaceholders((placeholderString, index) -> {
            queryParameters.add(originalParameters.get(index - 1));
            return "?";
        });

        PreparedStatement preparedStatement = connection.prepareStatement(queryTemplate);
        addPreparedStatementParameters(queryParameters, preparedStatement);
        return preparedStatement;
    }

    private void addPreparedStatementParameters(List<ITemplateParameter> queryParameters, PreparedStatement preparedStatement) throws SQLException, IllegalArgumentException {
        for (int i = 0; i < queryParameters.size(); i++) {
            ITemplateParameter parameter = queryParameters.get(i);

            switch (parameter.getParameterType()) {
                case INTEGER:
                    preparedStatement.setLong(i + 1, (long) parameter.getValue());
                    break;
                case STRING:
                    preparedStatement.setString(i + 1, (String) parameter.getValue());
                    break;
                case BOOLEAN:
                    preparedStatement.setBoolean(i + 1, (Boolean) parameter.getValue());
                    break;
                case DECIMAL:
                    preparedStatement.setBigDecimal(i + 1, (BigDecimal) parameter.getValue());
                    break;
                case DATETIME:
                    java.util.Date date = ((java.util.Date) parameter.getValue());
                    if (date == null)
                        preparedStatement.setTimestamp(i + 1, null);
                    else
                        preparedStatement.setTimestamp(i + 1, new Timestamp(date.getTime()));
                    break;
                default:
                    throw new IllegalArgumentException("Invalid parameter type: " + parameter.getParameterType());
            }
        }
    }
}

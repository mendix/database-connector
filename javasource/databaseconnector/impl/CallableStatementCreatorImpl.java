package databaseconnector.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.callablestatement.SqlParameter;
import databaseconnector.impl.callablestatement.SqlParameterDatetime;
import databaseconnector.impl.callablestatement.SqlParameterDecimal;
import databaseconnector.impl.callablestatement.SqlParameterLong;
import databaseconnector.impl.callablestatement.SqlParameterPrimitiveValue;
import databaseconnector.impl.callablestatement.SqlParameterString;
import databaseconnector.impl.callablestatement.StatementWrapper;
import databaseconnector.interfaces.CallableStatementCreator;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class CallableStatementCreatorImpl implements CallableStatementCreator {

	@Override
	public StatementWrapper create(final Statement statement, final Connection connection) throws SQLException {
		final IContext context = statement.getContext();
		final CallableStatement cStatement = connection.prepareCall(statement.getContent());

		List<SqlParameter> parameters = Core
				.retrieveByPath(context, statement.getMendixObject(),
						Parameter.MemberNames.Parameter_Statement.toString())
				.stream().map(p -> initialize(context, p)).collect(Collectors.toList());

		for (SqlParameter p : parameters) {
			p.prepareCall(cStatement);
		}

		return new StatementWrapper(cStatement, parameters);
	}

	public SqlParameter initialize(final IContext context, IMendixObject mendixObject) {
		SqlParameter ret = null;

		switch (mendixObject.getType()) {
		case ParameterDatetime.entityName:
			ret = new SqlParameterDatetime(context, mendixObject);
			break;
		case ParameterString.entityName:
			ret = new SqlParameterString(context, mendixObject);
			break;
		case ParameterLong.entityName:
			ret = new SqlParameterLong(context, mendixObject);
			break;
		case ParameterDecimal.entityName:
			ret = new SqlParameterDecimal(context, mendixObject);
			break;
		default:
			throw new IllegalArgumentException(
					String.format("Parameter type %s not supported.", mendixObject.getType()));
		}
		return ret;
	}
}

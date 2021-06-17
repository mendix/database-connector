package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterString;

public interface SqlParameter {
	void prepareCall(CallableStatement cStatement) throws SQLException;

	void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException;

	public static SqlParameter initialize(final IContext context, IMendixObject mendixObject) {
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
		case ParameterObject.entityName:
			List<SqlParameterPrimitiveValue<?>> fields = Core
					.retrieveByPath(context, mendixObject, Parameter.MemberNames.MemberOfObject.toString(), true)
					.stream().map(p -> (SqlParameterPrimitiveValue<?>) initialize(context, p)).sorted()
					.collect(Collectors.toList());
			ret = new SqlParameterObject(context, mendixObject, fields);
			break;
		default:
			throw new IllegalArgumentException(
					String.format("Parameter type %s not supported.", mendixObject.getType()));
		}
		return ret;
	}
}

package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.util.Date;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class SqlParameterDatetime extends SqlParameterPrimitiveValue<Date> {
	public SqlParameterDatetime(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DATE);
	}

	@Override
	protected Date getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getDate(name);
	}

	@Override
	protected Date getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getDate(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Date value) throws SQLException {
		cStatement.setDate(name, new java.sql.Date(value.getTime()));
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Date value) throws SQLException {
		cStatement.setDate(position, new java.sql.Date(value.getTime()));
	}
}

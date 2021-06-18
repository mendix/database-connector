package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class SqlParameterDatetime extends SqlParameterPrimitiveValue {
	public SqlParameterDatetime(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DATE);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getDate(name);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getDate(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Object value) throws SQLException {
		cStatement.setDate(name, new java.sql.Date(((java.util.Date) value).getTime()));
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Object value) throws SQLException {
		cStatement.setDate(position, new java.sql.Date(((java.util.Date) value).getTime()));
	}
}

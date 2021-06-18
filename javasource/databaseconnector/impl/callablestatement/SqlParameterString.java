package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class SqlParameterString extends SqlParameterPrimitiveValue {
	public SqlParameterString(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.VARCHAR);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getString(name);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getString(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Object value) throws SQLException {
		cStatement.setString(name, (String) value);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Object value) throws SQLException {
		cStatement.setString(position, (String) value);
	}
}

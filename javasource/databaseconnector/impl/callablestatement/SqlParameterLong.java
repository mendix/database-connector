package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterLong;

public class SqlParameterLong extends SqlParameterPrimitiveValue {
	public SqlParameterLong(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.NUMERIC);
	}

	@Override
	public void setMxObjectValue(Object value) {
		if (value instanceof BigDecimal) {
			((ParameterLong) this.parameterObject).setValue(((BigDecimal) value).longValue());
		} else {
			((ParameterLong) this.parameterObject).setValue(((Long) value));
		}
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getLong(name);
	}

	@Override
	protected Object getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getLong(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Object value) throws SQLException {
		cStatement.setLong(name, (Long) value);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Object value) throws SQLException {
		cStatement.setLong(position, (Long) value);
	}
}

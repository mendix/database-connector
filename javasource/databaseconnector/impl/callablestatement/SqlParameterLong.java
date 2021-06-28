package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.ParameterLong;

public class SqlParameterLong extends SqlParameterPrimitiveValue<Long> {
	public SqlParameterLong(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.NUMERIC);
	}

	@Override
	public void setValue(Object value) throws DatabaseConnectorException {
		if (value instanceof BigDecimal) {
			((ParameterLong) this.parameterObject).setValue(((BigDecimal) value).longValue());
		} else {
			super.setValue(value);
		}
	}

	@Override
	protected Long getStatementValue(CallableStatement cStatement, String name) throws SQLException {
		return cStatement.getLong(name);
	}

	@Override
	protected Long getStatementValue(CallableStatement cStatement, int position) throws SQLException {
		return cStatement.getLong(position);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, String name, Long value) throws SQLException {
		cStatement.setLong(name, value);
	}

	@Override
	protected void setStatementValue(CallableStatement cStatement, int position, Long value) throws SQLException {
		cStatement.setLong(position, value);
	}
}

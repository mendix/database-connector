package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterLong;

public class SqlParameterLong extends SqlParameterPrimitiveValue<ParameterLong> {
	public SqlParameterLong(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.NUMERIC);
	}

	@Override
	protected void setValueInput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null) {
			cStatement.setLong(index, this.mxObject.getValue());
		} else {
			cStatement.setLong(name, this.mxObject.getValue());
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement, int index, String name) throws SQLException {
		final Long value;
		if (name == null) {
			value = cStatement.getLong(index);
		} else {
			value = cStatement.getLong(name);
		}
		this.mxObject.setValue(value);
	}

	@Override
	public boolean isValueNull() {
		return this.mxObject.getValue() == null;
	}

	@Override
	protected Object getMxObjectValue() {
		return this.mxObject.getValue();
	}

	@Override
	protected void setMxObjectValue(Object value) {
		if (value instanceof Long) {
			this.mxObject.setValue((Long) value);
		} else if (value instanceof BigDecimal) {
			this.mxObject.setValue(((BigDecimal) value).longValue());
		} else {
			throw new IllegalArgumentException("Unable to set value of ParameterLong from " + value.toString());
		}
	}
}

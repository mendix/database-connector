package databaseconnector.impl.callablestatement;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterDecimal;

public class SqlParameterDecimal extends SqlParameterPrimitiveValue<ParameterDecimal> {
	public SqlParameterDecimal(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DECIMAL);
	}

	@Override
	protected void setValueInput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null) {
			cStatement.setBigDecimal(index, this.mxObject.getValue());
		} else {
			cStatement.setBigDecimal(name, this.mxObject.getValue());
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement, int index, String name) throws SQLException {
		final BigDecimal value;
		if (name == null) {
			value = cStatement.getBigDecimal(index);
		} else {
			value = cStatement.getBigDecimal(name);
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
		this.mxObject.setValue((BigDecimal) value);
	}
}

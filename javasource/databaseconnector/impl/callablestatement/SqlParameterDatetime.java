package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterDatetime;

public class SqlParameterDatetime extends SqlParameterPrimitiveValue<ParameterDatetime> {
	public SqlParameterDatetime(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.DATE);
	}

	@Override
	public void retrieveResult(CallableStatement cStatement, int index, String name) throws SQLException {
		final Date value;
		if (this.mxObject.getName() == null) {
			value = cStatement.getDate(index);
		} else {
			value = cStatement.getDate(name);
		}

		this.mxObject.setValue(value);
	}

	@Override
	protected void setValueInput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null) {
			cStatement.setDate(index, (Date) getMxObjectValue());
		} else {
			cStatement.setDate(name, (Date) getMxObjectValue());
		}
	}

	@Override
	protected boolean isValueNull() {
		return this.mxObject.getValue() == null;
	}

	@Override
	protected Object getMxObjectValue() {
		return this.mxObject.getValue() == null ? null
				: new java.sql.Date(this.mxObject.getValue().getTime());
	}

	@Override
	protected void setMxObjectValue(Object value) {
		this.mxObject.setValue((Date) value);
	}
}

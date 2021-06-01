package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterString;

public class SqlParameterString extends SqlParameterPrimitiveValue<ParameterString> {
	public SqlParameterString(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject, java.sql.Types.VARCHAR);
	}

	@Override
	protected void setValueInput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null) {
			cStatement.setString(index, this.mxObject.getValue());
		} else {
			cStatement.setString(name, this.mxObject.getValue());
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement, int index, String name) throws SQLException {
		final String value;
		if (name == null) {
			value = cStatement.getString(index);
		} else {
			value = cStatement.getString(name);
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
		this.mxObject.setValue((String) value);
	}
}

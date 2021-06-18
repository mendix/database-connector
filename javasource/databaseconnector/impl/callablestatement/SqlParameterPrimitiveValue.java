package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public abstract class SqlParameterPrimitiveValue extends SqlParameter {
	protected final int SQL_TYPE;

	@SuppressWarnings("unchecked")
	protected SqlParameterPrimitiveValue(final IContext context, IMendixObject mendixObject, int SQL_TYPE) {
		super(context, mendixObject);
		this.SQL_TYPE = SQL_TYPE;
	}

	public void prepareInput(CallableStatement cStatement) throws SQLException {
		String name = this.getName();
		if (this.getMxObjectValue() == null) {
			if (name == null || name.isBlank()) {
				cStatement.setNull(this.getPosition(), SQL_TYPE);
			} else {
				cStatement.setNull(name, SQL_TYPE);
			}
		} else {
			if (name == null || name.isBlank()) {
				setStatementValue(cStatement, this.getPosition(), getMxObjectValue());
			} else {
				setStatementValue(cStatement, name, getMxObjectValue());
			}
		}
	}

	public void prepareOutput(CallableStatement cStatement) throws SQLException {
		String name = this.getName();
		if (name == null || name.isBlank()) {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE);
		} else {
			cStatement.registerOutParameter(name, SQL_TYPE);
		}
	}

	@Override
	public Object getMxObjectValue() {
		return this.parameterObject.getMendixObject().getValue(this.parameterObject.getContext(), "Value");
	}
	
	@Override
	public void setMxObjectValue(Object value) {
		this.parameterObject.getMendixObject().setValue(this.parameterObject.getContext(), "Value", value);
	}

	@Override
	public void getValueOutput(CallableStatement cStatement) throws SQLException {
		final Object value;
		final String name = this.getName();
		if (name == null || name.isBlank()) {
			value = getStatementValue(cStatement, this.getPosition());
		} else {
			value = getStatementValue(cStatement, name);
		}
		setMxObjectValue(value);
	}
	

	protected abstract Object getStatementValue(CallableStatement cStatement, String name) throws SQLException;
	protected abstract Object getStatementValue(CallableStatement cStatement, int position) throws SQLException;

	protected abstract void setStatementValue(CallableStatement cStatement, String name, Object value) throws SQLException;
	protected abstract void setStatementValue(CallableStatement cStatement, int position, Object value) throws SQLException;
}

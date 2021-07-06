package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;

public abstract class SqlParameterPrimitiveValue<T> extends SqlParameter {
	protected final int SQL_TYPE;

	protected SqlParameterPrimitiveValue(final IContext context, IMendixObject mendixObject, int SQL_TYPE) {
		super(context, mendixObject);
		this.SQL_TYPE = SQL_TYPE;
	}

	public void prepareInput(CallableStatement cStatement) throws SQLException {
		String name = this.getName();
		if (this.getValue() == null) {
			if (name == null || name.isBlank()) {
				cStatement.setNull(this.getPosition(), SQL_TYPE);
			} else {
				cStatement.setNull(name, SQL_TYPE);
			}
		} else {
			if (name == null || name.isBlank()) {
				setStatementValue(cStatement, this.getPosition(), getValue());
			} else {
				setStatementValue(cStatement, name, getValue());
			}
		}
	}

	public void prepareOutput(CallableStatement cStatement) throws SQLException {
		if (this.isNameDefined()) {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE);
		} else {
			cStatement.registerOutParameter(this.getName(), SQL_TYPE);
		}
	}

	@Override
	public T getValue() {
		return this.parameterObject.getMendixObject().getValue(this.parameterObject.getContext(), "Value");
	}
	
	@Override
	public void setValue(Object value) throws DatabaseConnectorException {
		try {
			this.parameterObject.getMendixObject().setValue(this.parameterObject.getContext(), "Value", value);
		} catch (Exception e) {
			throw new DatabaseConnectorException(String.format("Unable to set value %s for parameter %s.", value.toString(), this.parameterObject.getMendixObject().getType()));
		}
	}

	@Override
	public void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		final T value;
		final String name = this.getName();
		if (name == null || name.isBlank()) {
			value = getStatementValue(cStatement, this.getPosition());
		} else {
			value = getStatementValue(cStatement, name);
		}
		setValue(value);
	}
	

	protected abstract T getStatementValue(CallableStatement cStatement, String name) throws SQLException;
	protected abstract T getStatementValue(CallableStatement cStatement, int position) throws SQLException;

	protected abstract void setStatementValue(CallableStatement cStatement, String name, T value) throws SQLException;
	protected abstract void setStatementValue(CallableStatement cStatement, int position, T value) throws SQLException;
}

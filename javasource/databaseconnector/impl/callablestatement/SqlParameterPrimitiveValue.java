package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;

/**
 * Wrapper class for all basic primitives. As they all mostly behave the same,
 * aside from <code>SQL_TYPE</code> integer and which methods to call to set or
 * get values in the CallableStatement.
 * 
 * If necessary, some conversion might be made from the value stored in Mendix
 * to the database (for instance, with Dates, or when reading numbers).
 */
public abstract class SqlParameterPrimitiveValue<T> extends SqlParameter {
	protected final int SQL_TYPE;

	protected SqlParameterPrimitiveValue(final IContext context, IMendixObject mendixObject, int SQL_TYPE) {
		super(context, mendixObject);
		this.SQL_TYPE = SQL_TYPE;
	}

	public void prepareInput(CallableStatement cStatement) throws SQLException {
		if (this.getValue() == null) {
			if (this.isNameDefined()) {
				cStatement.setNull(this.getName(), SQL_TYPE);
			} else {
				cStatement.setNull(this.getPosition(), SQL_TYPE);
			}
		} else {
			if (this.isNameDefined()) {
				setStatementValue(cStatement, this.getName(), getValue());
			} else {
				setStatementValue(cStatement, this.getPosition(), getValue());
			}
		}
	}

	public void prepareOutput(CallableStatement cStatement) throws SQLException {
		if (this.isNameDefined()) {
			cStatement.registerOutParameter(this.getName(), SQL_TYPE);
		} else {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE);
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
			throw new DatabaseConnectorException(String.format("Unable to set value %s for parameter %s.",
					value.toString(), this.parameterObject.getMendixObject().getType()));
		}
	}

	@Override
	public void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		final T value;
		if (this.isNameDefined()) {
			value = getStatementValue(cStatement, this.getName());
		} else {
			value = getStatementValue(cStatement, this.getPosition());
		}

		// Avoid automatic casts by the JDBC API of SQL NULL values by manually checking the previous value
		if (cStatement.wasNull()) {
			setValue(null);
		} else {
			setValue(value);
		}
	}

	protected abstract T getStatementValue(CallableStatement cStatement, String name) throws SQLException;

	protected abstract T getStatementValue(CallableStatement cStatement, int position) throws SQLException;

	protected abstract void setStatementValue(CallableStatement cStatement, String name, T value) throws SQLException;

	protected abstract void setStatementValue(CallableStatement cStatement, int position, T value) throws SQLException;
}

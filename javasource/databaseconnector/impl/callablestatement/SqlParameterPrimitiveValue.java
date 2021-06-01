package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.SQLException;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterMode;

public abstract class SqlParameterPrimitiveValue<P extends Parameter> implements SqlParameter, Comparable<SqlParameterPrimitiveValue<?>> {
	protected final P mxObject;
	protected final int SQL_TYPE;

	@SuppressWarnings("unchecked")
	protected SqlParameterPrimitiveValue(final IContext context, IMendixObject mendixObject, int SQL_TYPE) {
		this.mxObject = (P) Parameter.initialize(context, mendixObject);
		this.SQL_TYPE = SQL_TYPE;
		
		if ((this.mxObject.getName() == null || this.mxObject.getName().isBlank()) && this.mxObject.getPosition() == null) {
			throw new IllegalArgumentException("Parameter was initialized with neither name or position.");
		}
	}

	public void prepareCall(CallableStatement cStatement) throws SQLException {
		String name = mxObject.getName();
		int index = mxObject.getPosition();

		switch (mxObject.getParameterMode()) {
		case INPUT:
			prepareInput(cStatement, index, name);
			break;
		case OUTPUT:
			prepareOutput(cStatement, index, name);
			break;
		case INOUT:
			prepareInput(cStatement, index, name);
			prepareOutput(cStatement, index, name);
			break;
		default:
			throw new IllegalArgumentException(
					"Unrecognized parameter type" + mxObject.getParameterMode().toString());
		}
	}

	private void prepareInput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (isValueNull()) {
			if (name == null || name.isBlank()) {
				cStatement.setNull(index, SQL_TYPE);
			} else {
				cStatement.setNull(name, SQL_TYPE);
			}
		} else {
			setValueInput(cStatement, index, name);
		}
	}

	private void prepareOutput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null || name.isBlank()) {
			cStatement.registerOutParameter(index, SQL_TYPE);
		} else {
			cStatement.registerOutParameter(name, SQL_TYPE);
		}
	}

	public void retrieveResult(CallableStatement cStatement) throws SQLException {
		String name = mxObject.getName();
		int index = mxObject.getPosition();

		if (mxObject.getParameterMode().equals(ParameterMode.OUTPUT) || mxObject.getParameterMode().equals(ParameterMode.INOUT)) {
			retrieveResult(cStatement, index, name);
		}
	}
	
	protected abstract void setValueInput(CallableStatement cStatement, int index, String name) throws SQLException;
	protected abstract void retrieveResult(CallableStatement cStatement, int index, String name) throws SQLException;
	protected abstract boolean isValueNull();
	protected abstract Object getMxObjectValue();
	protected abstract void setMxObjectValue(Object value);

	@Override
	public int compareTo(SqlParameterPrimitiveValue<?> other) {
		return this.mxObject.getPosition() - other.mxObject.getPosition();
	}
}

package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;

public class SqlParameterObject implements SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.STRUCT;
	
	private final ParameterObject mxObject;
	private List<SqlParameterPrimitiveValue<?>> objectFields;

	public SqlParameterObject(final IContext context, IMendixObject mendixObject, List<SqlParameterPrimitiveValue<?>> objectFields) {
		this.mxObject = ParameterObject.initialize(context, mendixObject);
		this.objectFields = objectFields;
		
		if ((this.mxObject.getName() == null || this.mxObject.getName().isBlank()) && this.mxObject.getPosition() == null) {
			throw new IllegalArgumentException("Parameter was initialized with neither name or position.");
		}

		Set<Integer> positions = new HashSet<Integer>();
		for (SqlParameterPrimitiveValue<?> field : objectFields) {
			if (field.mxObject.getPosition() == null) {
				String objectNameOrPosition = (this.mxObject.getName() == null || this.mxObject.getName().isBlank()) ? this.mxObject.getPosition().toString() : this.mxObject.getName();
				throw new IllegalArgumentException(String.format("Missing position information for field of type %s of ParameterObject %s.", field.mxObject.getMendixObject().getType(), objectNameOrPosition));
			}
			
			if (positions.contains(field.mxObject.getPosition())) {
				throw new IllegalArgumentException(String.format("Duplicate field position in object parameter at position %d.", field.mxObject.getPosition()));
			}
			positions.add(field.mxObject.getPosition());
		}
	}

	@Override
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
		Struct objStruct = createConnectionStruct(cStatement.getConnection());
		
		if (objStruct == null) {
			if (name == null) {
				cStatement.setNull(index, SQL_TYPE);
			} else {
				cStatement.setNull(name, SQL_TYPE);
			}
		} else {
			if (name == null) {
				cStatement.setObject(index, objStruct);
			} else {
				cStatement.setObject(name, objStruct);
			}
		}
	}

	private Struct createConnectionStruct(Connection connection) throws SQLException {
		Object[] attrVals = this.objectFields.stream().map(SqlParameterPrimitiveValue::getMxObjectValue).toArray();
		return connection.createStruct(this.mxObject.getSQLTypeName(), attrVals);
	}

	private void prepareOutput(CallableStatement cStatement, int index, String name) throws SQLException {
		if (name == null || name.isBlank()) {
			cStatement.registerOutParameter(index, SQL_TYPE, this.mxObject.getSQLTypeName().toUpperCase());
		} else {
			cStatement.registerOutParameter(name, SQL_TYPE, this.mxObject.getSQLTypeName().toUpperCase());
		}
	}

	@Override
	public void retrieveResult(CallableStatement cStatement) throws SQLException {
		if (mxObject.getParameterMode().equals(ParameterMode.OUTPUT) || mxObject.getParameterMode().equals(ParameterMode.INOUT)) {
			Struct objStruct = retrieveResultStruct(cStatement);

			Object[] values = objStruct.getAttributes();

			int index = 0;
			for (SqlParameterPrimitiveValue<?> field : objectFields) {
				field.setMxObjectValue(values[index]);
				index++;
			}
		}
	}

	private Struct retrieveResultStruct(CallableStatement cStatement) throws SQLException {
		String name = mxObject.getName();
		
		if (name == null || name.isBlank()) {
			return (Struct) cStatement.getObject(this.mxObject.getPosition());
		} else {
			return (Struct) cStatement.getObject(name);
		}
	}
}

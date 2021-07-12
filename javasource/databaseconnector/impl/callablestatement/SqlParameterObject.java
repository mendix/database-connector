package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.ParameterObject;

/**
 * Wrapper class for objects, or STRUCT in JDBC terms.
 * 
 * If used as input, we expect all the fields to be in the inner's NPE
 * <code>ParameterObject_Parameter</code> association. All fields must have
 * a unique position, indicating the order we should (de-)serialize data from
 * the database.
 * 
 * If used as OUTPUT, the <code>ParameterObject_Parameter</code> association
 * may be left empty and we will attempt to guess the resulting types.
 */
public class SqlParameterObject extends SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.STRUCT;
	
	private List<SqlParameter> objectFields;

	public SqlParameterObject(final IContext context, IMendixObject mendixObject, List<SqlParameter> objectFields) {
		super(context, mendixObject);
		this.objectFields = objectFields;

		Set<Integer> positions = new HashSet<Integer>();
		for (SqlParameter field : objectFields) {
			if (field.getPosition() == null) {
				throw new IllegalArgumentException(String.format("Missing position information for field of type %s of ParameterObject %s.", field.parameterObject.getMendixObject().getType(), this.getNameOrPosition()));
			}

			if (positions.contains(field.getPosition())) {
				throw new IllegalArgumentException(String.format("Duplicate field position in object parameter at position %d.", field.getPosition()));
			}
			positions.add(field.getPosition());
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Struct objStruct = null;

		if (this.isNameDefined()) {
			objStruct = (Struct) cStatement.getObject(this.getName());
		} else {
			objStruct = (Struct) cStatement.getObject(this.getPosition());
		}
		this.setValue(objStruct);
	}

	@Override
	protected void prepareOutput(CallableStatement cStatement) throws SQLException {
		final String sqlTypeName = ((ParameterObject) this.parameterObject).getSQLTypeName();
		
		if (this.isNameDefined()) {
			cStatement.registerOutParameter(this.getName(), SQL_TYPE, sqlTypeName);
		} else {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE, sqlTypeName);
		}
	}

	@Override
	protected void prepareInput(CallableStatement cStatement) throws SQLException {
		Struct objStruct = createConnectionStruct(cStatement.getConnection());

		if (objStruct == null) {
			if (this.isNameDefined()) {
				cStatement.setNull(this.getName(), SQL_TYPE);
			} else {
				cStatement.setNull(this.getPosition(), SQL_TYPE);
			}
		} else {
			if (this.isNameDefined()) {
				cStatement.setObject(this.getName(), objStruct);
			} else {
				cStatement.setObject(this.getPosition(), objStruct);
			}
		}
	}

	@Override
	List<SqlParameter> getValue() {
		return this.objectFields;
	}

	@Override
	void setValue(Object value) throws DatabaseConnectorException {
		try {
			Object[] valArray = ((Struct) value).getAttributes();
			if (this.objectFields.isEmpty()) {
				for (int i = 0; i < valArray.length; i++) {
					this.objectFields.add(SqlParameter.createParameterFromValue(this.parameterObject.getContext(), this.getParameterMode(), i+1, valArray[i]));
				}
				((ParameterObject) this.parameterObject).setParameterObject_Parameter(this.objectFields.stream().map(p -> p.parameterObject).collect(Collectors.toList()));
			} else {
				if (valArray.length != this.objectFields.size()) {
					throw new DatabaseConnectorException(String.format("Number of values for object do not match number of expected fields. Expected %d, retrieved %d.", this.objectFields.size(), valArray.length));
				}

				int index = 0;
				for (SqlParameter field : objectFields) {
					field.setValue(valArray[index]);
					index++;
				}
			}
		} catch (SQLException e) {
			throw new DatabaseConnectorException("Unable to set values for object", e);
		}
	}

	private Struct createConnectionStruct(Connection connection) throws SQLException {
		Object[] attrVals = this.objectFields.stream().map(SqlParameter::getValue).toArray();
		String sqlTypeName = ((ParameterObject) this.parameterObject).getSQLTypeName();
		return connection.createStruct(sqlTypeName, attrVals);
	}
}

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
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterObject;

public class SqlParameterObject extends SqlParameter<List<SqlParameter<?>>> {
	private final static int SQL_TYPE = java.sql.Types.STRUCT;
	
	private List<SqlParameter<?>> objectFields;

	public SqlParameterObject(final IContext context, IMendixObject mendixObject, List<SqlParameter<?>> objectFields) {
		super(context, mendixObject);
		this.objectFields = objectFields;

		Set<Integer> positions = new HashSet<Integer>();
		for (SqlParameter<?> field : objectFields) {
			if (field.getPosition() == null) {
				String objectNameOrPosition = (this.parameterObject.getName() == null || this.parameterObject.getName().isBlank()) ? this.parameterObject.getPosition().toString() : this.parameterObject.getName();
				throw new IllegalArgumentException(String.format("Missing position information for field of type %s of ParameterObject %s.", field.parameterObject.getMendixObject().getType(), objectNameOrPosition));
			}

			if (positions.contains(field.getPosition())) {
				throw new IllegalArgumentException(String.format("Duplicate field position in object parameter at position %d.", field.getPosition()));
			}
			positions.add(field.getPosition());
		}
	}


	@Override
	protected void getValueOutput(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		Struct objStruct = retrieveResultStruct(cStatement);

		Object[] values = objStruct.getAttributes();
		if (values.length != this.objectFields.size()) {
			throw new DatabaseConnectorException(String.format("Number of values for object do not match number of expected fields. Expected %d, retrieved %d.", this.objectFields.size(), values.length));
		}

		try {
			int index = 0;
			for (SqlParameter<?> field : objectFields) {
				field.setValue(values[index]);
				index++;
			}
		} catch (DatabaseConnectorException | IllegalArgumentException e) {
			throw new DatabaseConnectorException("Unable to set field of ParameterObject", e);
		}
	}

	@Override
	protected void prepareOutput(CallableStatement cStatement) throws SQLException {
		final String name = this.getName();
		final String sqlTypeName = ((ParameterObject) this.parameterObject).getSQLTypeName();
		
		if (name == null || name.isBlank()) {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE, sqlTypeName);
		} else {
			cStatement.registerOutParameter(name, SQL_TYPE, sqlTypeName);
		}
	}

	@Override
	protected void prepareInput(CallableStatement cStatement) throws SQLException {
		Struct objStruct = createConnectionStruct(cStatement.getConnection());
		final String name = this.getName();
		
		if (objStruct == null) {
			if (name == null || name.isBlank()) {
				cStatement.setNull(this.getPosition(), SQL_TYPE);
			} else {
				cStatement.setNull(name, SQL_TYPE);
			}
		} else {
			if (name == null) {
				cStatement.setObject(this.getPosition(), objStruct);
			} else {
				cStatement.setObject(name, objStruct);
			}
		}
	}

	@Override
		// TODO Auto-generated method stub
		return null;
	List<SqlParameter<?>> getValue() {
	}

	@Override
		// TODO Auto-generated method stub
		
	void setValue(Object value) throws DatabaseConnectorException {
	}

	private Struct createConnectionStruct(Connection connection) throws SQLException {
		Object[] attrVals = this.objectFields.stream().map(SqlParameter::getValue).toArray();
		String sqlTypeName = ((ParameterObject) this.parameterObject).getSQLTypeName();
		return connection.createStruct(sqlTypeName, attrVals);
	}

	private Struct retrieveResultStruct(CallableStatement cStatement) throws SQLException {
		String name = this.getName();
		
		if (name == null || name.isBlank()) {
			return (Struct) cStatement.getObject(this.getPosition());
		} else {
			return (Struct) cStatement.getObject(name);
		}
	}

}

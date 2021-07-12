package databaseconnector.impl.callablestatement;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

import databaseconnector.impl.DatabaseConnectorException;
import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterRefCursor;

/**
 * Wrapper class for cursors.
 * 
 * Cannot be used as input (INPUT or INOUT).
 * 
 * The current implementation reads all data at once, and there is no pagination.
 * In effect, this cursor behaves as a ParameterList.
 */
public class SqlParameterRefCursor extends SqlParameter {
	private final static int SQL_TYPE = java.sql.Types.REF_CURSOR;
	private List<SqlParameterObject> result = new ArrayList<SqlParameterObject>();

	public SqlParameterRefCursor(final IContext context, IMendixObject mendixObject) {
		super(context, mendixObject);

		if (!this.getParameterMode().equals(ParameterMode.OUTPUT)) {
			throw new IllegalArgumentException("RefCursor can only be used in OUTPUT mode.");
		}
	}

	@Override
	protected void prepareInput(CallableStatement cStatement) throws SQLException {
		throw new IllegalArgumentException("Trying to prepare input for Ref Cursor.");
	}

	@Override
	protected void prepareOutput(CallableStatement cStatement) throws SQLException {
		if (this.isNameDefined()) {
			cStatement.registerOutParameter(this.getName(), SQL_TYPE);
		} else {
			cStatement.registerOutParameter(this.getPosition(), SQL_TYPE);
		}
	}

	@Override
	protected void retrieveResult(CallableStatement cStatement) throws SQLException, DatabaseConnectorException {
		IContext context = this.parameterObject.getContext();
		try (ResultSet rs = retrieveResultSet(cStatement)) {
			int numColumns = rs.getMetaData().getColumnCount(); 

			int index = 0;
			while (rs.next()) {
				List<SqlParameter> fields = new ArrayList<SqlParameter>(numColumns);
				for (int column = 1; column <= numColumns; column++) {
					String columnName = rs.getMetaData().getColumnLabel(column);
					int typeHint = rs.getMetaData().getColumnType(column);
					SqlParameter newParameter = SqlParameter.createParameterFromValue(context, this.getParameterMode(), column, rs.getObject(column), typeHint);
					newParameter.parameterObject.setName(columnName);
					fields.add(newParameter);
				}

				// Create the object to hold the fields
				IMendixObject newObject = Core.instantiate(context, ParameterObject.entityName);
				newObject.setValue(context, Parameter.MemberNames.ParameterMode.toString(), ParameterMode.OUTPUT.toString());
				newObject.setValue(context, Parameter.MemberNames.Position.toString(), index+1);
				SqlParameterObject valueSqlParameter = new SqlParameterObject(context, newObject, fields);

				// Point the fields to this new object
				List<Parameter> fieldMxObjects = fields.stream().map(p -> p.parameterObject).collect(Collectors.toList());
				((ParameterObject) valueSqlParameter.parameterObject).setParameterObject_Parameter(fieldMxObjects);

				this.result.add(valueSqlParameter);
				index++;
			}
		}
		
		// Point the newly loaded data to this object to this new object
		List<Parameter> resultMxObject = result.stream().map(p -> p.parameterObject).collect(Collectors.toList());
		((ParameterRefCursor) this.parameterObject).setParameterRefCursor_Parameter(resultMxObject);
	}

	private ResultSet retrieveResultSet(CallableStatement cStatement) throws SQLException {
		if (this.isNameDefined()) {
			return cStatement.getObject(this.getName(), ResultSet.class);
		} else {
			return cStatement.getObject(this.getPosition(), ResultSet.class);
		}
	}

	@Override
	List<SqlParameterObject> getValue() {
		return this.result;
	}

	@Override
	void setValue(Object value) throws DatabaseConnectorException {
		throw new DatabaseConnectorException("Unable to setValue in CURSOR types.");
	}
}

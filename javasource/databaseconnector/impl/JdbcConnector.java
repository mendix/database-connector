package databaseconnector.impl;

import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.MendixRuntimeException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaObject;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive;
import com.mendix.systemwideinterfaces.core.meta.IMetaPrimitive.PrimitiveType;
import com.mendix.systemwideinterfaces.javaactions.parameters.IStringTemplate;
import databaseconnector.interfaces.ConnectionManager;
import databaseconnector.interfaces.ObjectInstantiator;
import databaseconnector.interfaces.PreparedStatementCreator;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * JdbcConnector implements the execute query (and execute statement)
 * functionality, and returns a {@link Stream} of {@link IMendixObject}s.
 */
public class JdbcConnector {
	private final ILogNode logNode;
	private final ObjectInstantiator objectInstantiator;
	private final ConnectionManager connectionManager;
	private final PreparedStatementCreator preparedStatementCreator;

	public JdbcConnector(final ILogNode logNode, final ObjectInstantiator objectInstantiator,
						 final ConnectionManager connectionManager, final PreparedStatementCreator preparedStatementCreator) {
		this.logNode = logNode;
		this.objectInstantiator = objectInstantiator;
		this.connectionManager = connectionManager;
		this.preparedStatementCreator = preparedStatementCreator;
	}

	public JdbcConnector(final ILogNode logNode) {
		this(logNode, new ObjectInstantiatorImpl(), ConnectionManagerSingleton.getInstance(), new PreparedStatementCreatorImpl());
	}

	public Stream<IMendixObject> executeQuery(final String jdbcUrl, final String userName, final String password,
											  final IMetaObject metaObject, final String sql, final IContext context) throws SQLException {
		logNode.trace(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

		try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
			 PreparedStatement preparedStatement = preparedStatementCreator.create(sql, connection);
			 ResultSet resultSet = preparedStatement.executeQuery()) {
			ResultSetReader resultSetReader = new ResultSetReader(resultSet, metaObject);
			return resultSetReader.readAll().stream().map(CreateMendixObjectConverter(context, metaObject));
		}
	}

	public Stream<IMendixObject> executeQuery(final String jdbcUrl, final String userName, final String password,
											  final IMetaObject metaObject, final IStringTemplate sql, final IContext context) throws SQLException {
		logNode.trace(String.format("executeQuery: %s, %s, %s", jdbcUrl, userName, sql));

		try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
			 PreparedStatement preparedStatement = preparedStatementCreator.create(sql, connection);
			 ResultSet resultSet = preparedStatement.executeQuery()) {
			ResultSetReader resultSetReader = new ResultSetReader(resultSet, metaObject);
			return resultSetReader.readAll().stream().map(CreateMendixObjectConverter(context, metaObject));
		}
	}

	private Function<Object, Object> toSuitableValue(final PrimitiveType type) {
		return v -> type == PrimitiveType.Binary ? new ByteArrayInputStream((byte[]) v) : v;
	}

	private Function<Map<String, Optional<Object>>, IMendixObject> CreateMendixObjectConverter(final IContext context, final IMetaObject metaObject) {
		return columns -> {
			String entityName = metaObject.getName();
			IMendixObject obj = objectInstantiator.instantiate(context, entityName);

			BiConsumer<String, Optional<Object>> setMemberValue = (name, value) -> {
				IMetaPrimitive metaPrimitive = metaObject.getMetaPrimitive(name);
				if (metaPrimitive == null) {
					String errorMessage = String.format(
							"Database attribute '%1$s' is not in the entity '%2$s'."
									+ " Please check the entity '%2$s' attribute names with the database column names.",
							name, entityName);
					logNode.error(errorMessage);
					throw new MendixRuntimeException(errorMessage);
				}
				PrimitiveType type = metaPrimitive.getType();
				// convert to suitable value (different for Binary type)
				Function<Object, Object> toSuitableValue = toSuitableValue(type);
				// for Boolean type, convert null to false
				Supplier<Object> defaultValue = () -> type == PrimitiveType.Boolean ? Boolean.FALSE : null;
				// apply two functions declared above
				Object convertedValue = value.map(toSuitableValue).orElseGet(defaultValue);
				// update object with converted value
				if (type == PrimitiveType.HashString)
					throw new MendixRuntimeException(String.format(
							"Attribute type Hashed String for attribute '%1$s' on entity '%2$s' is not supported, "
									+ "please use attribute type 'String' instead",
							name, entityName));
				else
					obj.setValue(context, name, convertedValue);
			};

			columns.forEach(setMemberValue);
			logNode.trace("Instantiated object: " + obj);
			return obj;
		};
	}

	public long executeStatement(final String jdbcUrl, final String userName, final String password, final String sql)
			throws SQLException {
		logNode.trace(String.format("executeStatement: %s, %s, %s", jdbcUrl, userName, sql));

		try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
			 PreparedStatement preparedStatement = preparedStatementCreator.create(sql, connection)) {
			return preparedStatement.executeUpdate();
		}
	}

	public long executeStatement(final String jdbcUrl, final String userName, final String password, final IStringTemplate sql)
			throws SQLException {
		logNode.trace(String.format("executeStatement: %s, %s, %s", jdbcUrl, userName, sql));

		try (Connection connection = connectionManager.getConnection(jdbcUrl, userName, password);
			 PreparedStatement preparedStatement = preparedStatementCreator.create(sql, connection)) {
			return preparedStatement.executeUpdate();
		}
	}
}

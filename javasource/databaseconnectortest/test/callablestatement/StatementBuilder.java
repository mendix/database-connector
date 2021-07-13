package databaseconnectortest.test.callablestatement;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.mendix.core.Core;
import com.mendix.systemwideinterfaces.core.IContext;

import databaseconnector.proxies.Parameter;
import databaseconnector.proxies.ParameterDatetime;
import databaseconnector.proxies.ParameterDecimal;
import databaseconnector.proxies.ParameterList;
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
import databaseconnector.proxies.ParameterRefCursor;
import databaseconnector.proxies.ParameterString;
import databaseconnector.proxies.Statement;

public class StatementBuilder {
		
	private IContext context;
	private Statement statement;
	
	private List<Parameter> parameters = new LinkedList<Parameter>();
	
	public StatementBuilder(IContext context, Statement statement) {
		this.context = context;
		this.statement = statement;
	}
	
	public StatementBuilder(IContext context) {
		this.context = context;
		this.statement = Statement.initialize(context, Core.instantiate(context, Statement.entityName));
	}
	
	public Statement getStatement() {
		// Ensure the statement has the correct parameters:
		this.statement.setStatement_Parameter(this.parameters);

		return this.statement;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Parameter> T getParameter(int i) {
		return (T)parameters.get(i);
	}

	public StatementBuilder withContentFromFile(String fileName) throws IOException {
		Path fullPath = Core.getConfiguration().getResourcesPath().toPath().resolve(fileName);
		return this.withContent(Files.readString(fullPath));
	}

	public StatementBuilder withContent(String content) {
		statement.setContent(content);
		return this;
	}

	public <T> StatementBuilder withInputParameter(Integer position, String name, T value, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, value, ParameterMode.INPUT, parameterClass);
	}
	
	public <T> StatementBuilder withObjectInputParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initObjectParameter(position, name, value, ParameterMode.INPUT, sqlTypeName);
		parameters.add(parameter);
	
		return this;
	}
	
	public <T extends Parameter> StatementBuilder withListInputParameter(Integer position, String name, List<T> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initListParameter(position, name, value, ParameterMode.INPUT, sqlTypeName);
		parameters.add(parameter);
	
		return this;
	}

	public <T> StatementBuilder withOutputParameter(Integer position, String name, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, null, ParameterMode.OUTPUT, parameterClass);
	}
	
	public <T> StatementBuilder withObjectOutputParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initObjectParameter(position, name, value, ParameterMode.OUTPUT, sqlTypeName);
		parameters.add(parameter);
	
		return this;
	}
	
	public <T> StatementBuilder withListOutputParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initListParameter(position, name, value, ParameterMode.OUTPUT, sqlTypeName);
		parameters.add(parameter);
	
		return this;
	}

	public <T> StatementBuilder withRefCursorParameter(Integer position, String name, ParameterMode mode) throws Exception {
		
		Parameter parameter = initRefCursorParameter(position, name, mode);
		parameters.add(parameter);
	
		return this;
	}
	
	public <T> StatementBuilder withInOutParameter(Integer position, String name, T value, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, value, ParameterMode.INOUT, parameterClass);
	}
	
	public <T> StatementBuilder withObjectInOutParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initObjectParameter(position, name, value, ParameterMode.INOUT, sqlTypeName);
		parameters.add(parameter);
	
		return this;
	}
	
	private <T> StatementBuilder withParameter(Integer position, String name, T value, ParameterMode parameterMode, Class<?> parameterClass) throws Exception {
		
		Parameter parameter;

		
		if (parameterClass == ParameterDatetime.class) {
			parameter = initDatetimeParameter(position, name, (Date)value, parameterMode);
		} else if (parameterClass == ParameterDecimal.class) {
			parameter = initDecimalParameter(position, name, (BigDecimal)value, parameterMode);
		} else if (parameterClass == ParameterLong.class) {
			parameter = initLongParameter(position, name, (Long)value, parameterMode);
		} else if (parameterClass == ParameterString.class) {
			parameter = initStringParameter(position, name, (String)value, parameterMode);
		} else {
			throw new Exception("Unexpected parameter type");
		}
		
		parameters.add(parameter);
	
		return this;
	}
	
	public ParameterObject initObjectParameter(Integer position, String name, List<Parameter> value, ParameterMode parameterMode, String sqlTypeName) throws Exception {
		ParameterObject parameter = ParameterObject.initialize(context, Core.instantiate(context, ParameterObject.entityName));
		ParameterObject parameterInitialized = initParameter(position, name, parameterMode, parameter);
		parameterInitialized.setSQLTypeName(sqlTypeName);

		if (value != null) {
			parameterInitialized.setParameterObject_Parameter((List<Parameter>) value);
		}

		return parameterInitialized;
	}

	public <T extends Parameter> ParameterList initListParameter(Integer position, String name, List<T> value, ParameterMode parameterMode, String sqlTypeName) throws Exception {
		ParameterList parameter = ParameterList.initialize(context, Core.instantiate(context, ParameterList.entityName));
		ParameterList parameterInitialized = initParameter(position, name, parameterMode, parameter);
		parameterInitialized.setSQLTypeName(sqlTypeName);

		if (value != null) {
			parameterInitialized.setParameterList_Parameter((List<Parameter>) value);
		}

		return parameterInitialized;
	}
	
	public ParameterRefCursor initRefCursorParameter(Integer position, String name, ParameterMode parameterMode) throws Exception {
		ParameterRefCursor parameter = ParameterRefCursor.initialize(context, Core.instantiate(context, ParameterRefCursor.entityName));
		ParameterRefCursor parameterInitialized = initParameter(position, name, parameterMode, parameter);

		return parameterInitialized;
	}
	
	public ParameterDatetime datetimeField(Integer position, Date value) throws Exception {
		return initDatetimeParameter(position, null, value, ParameterMode.INPUT);
	}
	
	public ParameterDecimal decimalField(Integer position, BigDecimal value) throws Exception {
		return initDecimalParameter(position, null, value, ParameterMode.INPUT);

	}
	
	public ParameterLong longField(Integer position, Long value) throws Exception {
		return initLongParameter(position, null, value, ParameterMode.INPUT);
	}
	
	public ParameterString stringField(Integer position, String value) throws Exception {
		return initStringParameter(position, null, value, ParameterMode.INPUT);
	}
	
	private ParameterDatetime initDatetimeParameter(Integer position, String name, Date value, ParameterMode parameterMode) throws Exception {
		ParameterDatetime parameter = ParameterDatetime.initialize(context, Core.instantiate(context, ParameterDatetime.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		parameter.setValue(value);
		return parameter;
	}
	
	private ParameterDecimal initDecimalParameter(Integer position, String name, BigDecimal value, ParameterMode parameterMode) throws Exception {
		ParameterDecimal parameter = ParameterDecimal.initialize(context, Core.instantiate(context, ParameterDecimal.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		parameter.setValue(value);
		return parameter;
	}
	
	private ParameterLong initLongParameter(Integer position, String name, Long value, ParameterMode parameterMode) throws Exception {
		ParameterLong parameter = ParameterLong.initialize(context, Core.instantiate(context, ParameterLong.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		parameter.setValue(value);
		return parameter;
	}
	
	private ParameterString initStringParameter(Integer position, String name, String value, ParameterMode parameterMode) throws Exception {
		ParameterString parameter = ParameterString.initialize(context, Core.instantiate(context, ParameterString.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		parameter.setValue(value);
		return parameter;
	}
	
	private <T extends Parameter> T initParameter(Integer position, String name, ParameterMode parameterMode, T parameter) throws Exception {
		
		parameter.setParameterMode(context, parameterMode);
		if (position != null) parameter.setPosition(position);
		if (name != null) parameter.setName(name);

		return parameter;
	}
}

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
import databaseconnector.proxies.ParameterLong;
import databaseconnector.proxies.ParameterMode;
import databaseconnector.proxies.ParameterObject;
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
		parameter.setParameter_Statement(statement);
	
		return this;
	}

	public <T> StatementBuilder withOutputParameter(Integer position, String name, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, null, ParameterMode.OUTPUT, parameterClass);
	}
	
	public <T> StatementBuilder withObjectOutputParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initObjectParameter(position, name, value, ParameterMode.OUTPUT, sqlTypeName);
		parameters.add(parameter);
		parameter.setParameter_Statement(statement);
	
		return this;
	}
	
	public <T> StatementBuilder withInOutParameter(Integer position, String name, T value, Class<?> parameterClass) throws Exception {
		return this.withParameter(position, name, value, ParameterMode.INOUT, parameterClass);
	}
	
	public <T> StatementBuilder withObjectInOutParameter(Integer position, String name, List<Parameter> value, String sqlTypeName) throws Exception {
		
		Parameter parameter = initObjectParameter(position, name, value, ParameterMode.INOUT, sqlTypeName);
		parameters.add(parameter);
		parameter.setParameter_Statement(statement);
	
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
		parameter.setParameter_Statement(statement);
	
		return this;
	}
	
	public ParameterDatetime initDatetimeParameter(Integer position, String name, Date value, ParameterMode parameterMode) throws Exception {
		ParameterDatetime parameter = ParameterDatetime.initialize(context, Core.instantiate(context, ParameterDatetime.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		if (value != null) parameter.setValue(value);
		return parameter;
	}
	
	public ParameterDecimal initDecimalParameter(Integer position, String name, BigDecimal value, ParameterMode parameterMode) throws Exception {
		ParameterDecimal parameter = ParameterDecimal.initialize(context, Core.instantiate(context, ParameterDecimal.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		if (value != null) parameter.setValue(value);
		return parameter;
	}
	
	public ParameterLong initLongParameter(Integer position, String name, Long value, ParameterMode parameterMode) throws Exception {
		ParameterLong parameter = ParameterLong.initialize(context, Core.instantiate(context, ParameterLong.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		if (value != null) parameter.setValue(value);
		return parameter;
	}
	
	public ParameterString initStringParameter(Integer position, String name, String value, ParameterMode parameterMode) throws Exception {
		ParameterString parameter = ParameterString.initialize(context, Core.instantiate(context, ParameterString.entityName));
		parameter = initParameter(position, name, parameterMode, parameter);
		if (value != null) parameter.setValue(value);
		return parameter;
	}
	
	public ParameterObject initObjectParameter(Integer position, String name, List<Parameter> value, ParameterMode parameterMode, String sqlTypeName) throws Exception {
		ParameterObject parameter = ParameterObject.initialize(context, Core.instantiate(context, ParameterObject.entityName));
		ParameterObject parameterInitialized = initParameter(position, name, parameterMode, parameter);
		parameterInitialized.setSQLTypeName(sqlTypeName);

		if (value != null) {
			value.forEach(paramValue -> paramValue.setMemberOfObject(parameterInitialized));
		}
		
		return parameterInitialized;
	}
	
	private <T extends Parameter> T initParameter(Integer position, String name, ParameterMode parameterMode, T parameter) throws Exception {
		
		parameter.setParameterMode(context, parameterMode);
		if (position != null) parameter.setPosition(position);
		if (name != null) parameter.setName(name);

		return parameter;
	}
}

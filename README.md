# DatabaseConnector

The Mendix platform offers many ways to integrate with external data, but integrating with external databases has not been a seamless experience until now. 
The **DatabaseConnector** module can be used to seamlessly connect to external databases without limiting you in your choice of database or SQL dialect, enabling you to incorporate your external data directly in your Mendix application.
This readme will focus on executing SQL (Structured Query Language) Select Query and executing SQL Statements on **relational external databases**. 

**Execute query** connector (present in the DatabaseConnector module) provides a consistent environment for Mendix projects to perform an arbitrary 
Select SQL query on relational external databases. JDBC (Java Database Connectivity) API, a standard Java API, 
is used when this Java action attempts to connect with a Relational Database for which a JDBC driver exists.

**Execute statement** connector (present in the DatabaseConnector module) internally works in same manner as **Execute query** connector.
However, it is used for CREATE, INSERT, UPDATE, STORED PROCEDURE or DELETE SQL statements.

# Dependencies
* HikariCP-2.4.6.jar

# Installation
### Common Prerequisities
* A database **URL** address that points to your database.
* The **username** for logging into the database, relative to the database URL address.
* The **password** for logging into the database, relative to the database URL address.
* The JDBC driver jars, for the databases you want to connect to, must be placed inside the userlib directory of your Mendix application. 
  So if you want to connect to Amazon RDS PostgreSQL database (For e.g. *jdbc:postgresql://xyz-rds-instance.ccnapcvoeosh.eu-west-1.rds.amazonaws.com:5432/postgres*), 
  you need to place PostgreSQL Jdbc driver jar inside the userlib folder.

### Prerequisities for Execute query connector
* The Select SQL **query** to be performed, relative to the database type (*SQL dialect* differs for different databases).
* An instance of the resulting object. This instance is used only for defining the type of object to be returned.
* Add an entity in the domain model of your Mendix application. This new entity should inherit from the Row entity which already exists in the domain model of DatabaseConnector module.
  
### Prerequisities for Execute statement connector
* The SQL statement to execute, relative to the database type (SQL dialect differs for different databases).

## Installation
Import the module **DatabaseConnector** in your project (from the Mendix AppStore or by downloading and exporting the module from this project)

# Getting Started
Once you have imported the DatabaseConnector module in your mendix application. You will have *Database connector* available under
the connectors kit (in Toolbox section). It supports two connectors *Execute query* and *Execute statement*.
In order to use any of these in your mendix application, you can just drag and drop them to your mendix microflow as an activity.
Next step would be to provide all the arguments to the selected connector and choose the connector output result name. 

### Execute query connector
The result of this connector is a list of objects of type Row, which is also the output of the SELECT SQL query.

### Execute statement connector
The result of this connector is either an Integer or a Long value which usually represents the amount of affected rows.

# Known Issues
Proper security must be applied as this action can allow SQL Injection in your Mendix application.

# License
Licensed under the Apache license

# Developers notes
* git clone this repository
* Open the DatabaseConnector.mpr with your mendix modeler.
* Use *Deploy for Eclipse* option (shortcut F6) and you can then import this module as an eclipse project to the eclipse IDE.

# Version history
None

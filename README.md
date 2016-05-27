# Database Connector

The Mendix platform offers many ways to integrate with external data, but integrating with external databases has not been a seamless experience until now. 
The **Database connector** module can be used to seamlessly connect to external databases without limiting you in your choice of database or SQL dialect, enabling you to incorporate your external data directly in your Mendix application.
This document will focus on executing an SQL (Structured Query Language) Select Query and executing an SQL Statement on **external relational databases**. 

The **Execute query** action (present in the Database connector) provides a consistent environment for Mendix projects to perform an arbitrary Select SQL query on relational external databases. JDBC (Java Database Connectivity) API, a standard Java API, 
is used when this Java action attempts to connect with a relational database for which a JDBC driver exists.

The **Execute statement** action (present in the Database connector) internally works in the same manner as **Execute query** action.
However, it is used for INSERT, UPDATE, DELETE, STORED PROCEDURE or DDL statements.

# Dependencies
* HikariCP-2.4.6.jar

# Installation
### Prerequisities
* A database **URL** address that points to your database.
* The **username** for logging into the database, relative to the database URL address.
* The **password** for logging into the database, relative to the database URL address.
* The JDBC driver jars, for the databases you want to connect to, must be placed inside the userlib directory of your Mendix application. 
So if you want to connect to Amazon RDS PostgreSQL database (For e.g. *jdbc:postgresql://xyz-rds-instance.ccnapcvoeosh.eu-west-1.rds.amazonaws.com:5432/postgres*), 
you need to place PostgreSQL Jdbc driver jar inside the userlib folder.

### Extra Prerequisities for Execute query action
Let's say that you want to execute a query like `select name, number from stock`. This query has two columns of type String and Integer respectively. In order to use the Execute query action, you have to add an entity in the domain model that has the same attributes as the columns in the query.
This new entity should inherit from the Row entity which already exists in the domain model of Database connector module.


## Installation
Import the module **Database connector** in your project (from the Mendix AppStore or by downloading and exporting the module from this project)

# Getting Started
Once you have imported the DatabaseConnector module in your mendix application, you will have *Database connector* available in the Toolbox. It supports two actions *Execute query* and *Execute statement*.
In order to use any of these in your Mendix application, you can just drag and drop them to your microflow.
Next step would be to provide all the arguments to the selected action and choose the output result name. 

### Execute query action
The result of this action is a list of objects of type Row, which is also the output of the SELECT SQL query.

### Execute statement action
The result of this action is either an Integer or a Long value which usually represents the amount of affected rows.

# Known Issues
Proper security must be applied as this action can allow SQL Injection in your Mendix application.

# License
Licensed under the Apache license

# Developers notes
* `git clone https://github.com/mendix/database-connector.git`
* Open the DatabaseConnector.mpr with your mendix modeler.
* Use *Deploy for Eclipse* option (shortcut F6) and you can then import this module as an Eclipse project to the Eclipse IDE.

# Version history
None

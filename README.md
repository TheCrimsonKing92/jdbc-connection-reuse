# jdbc-connection-reuse

This project was built with JDK 17, but can likely run under any JDK >= 8.

## Setup

### Driver

The default build.gradle only accounts for the PostgreSQL driver. Add any further drivers you wish to use for your particular RDBMS.

### Properties

The project needs to be pointed at a database with a username, password, and URL - do so in the connection.properties file.
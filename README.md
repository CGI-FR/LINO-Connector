# LINO HTTP Connector

## Usage

### Configure the connector

Configure a datasource in the `application.properties` file.

```properties
# Example for Postgresql datasource
spring.datasource.url=jdbc:postgresql://localhost:5432/postgres
spring.datasource.username=postgres
spring.datasource.password=sakila
spring.jpa.properties.hibernate.dialect=com.cgi.lino.connector.postgresql.PostgreSQLCustomDialect
```

```properties
# Example for H2 datasource
#spring.datasource.url=jdbc:h2:tcp://localhost:1521/test
#spring.datasource.username=sa
#spring.datasource.password=
```

Configure the connection pool in the `application.properties` file.

```properties
spring.datasource.hikari.connection-timeout=20000
spring.datasource.hikari.minimum-idle=10
spring.datasource.hikari.maximum-pool-size=100
spring.datasource.hikari.idle-timeout=10000
spring.datasource.hikari.max-lifetime=30000
spring.datasource.hikari.auto-commit=true
```

Configure the server port in the `application.properties` file.

```properties
server.port=8080
```

### Launch the connector

```console
$ mvn spring-boot:run 
```

### Connect LINO

```console
$ lino dc add mydatasource http://localhost:8080/api/v1
succesfully added dataconnector.
```

### Use LINO

```console
$ lino table extract mydatasource
$ lino relation extract mydatasource
$ lino id create <start table>
$ lino pull mydatasource
$ lino push mydatasource
```

## Compatibility

This connector is compatible with LINO v1.7.0+.

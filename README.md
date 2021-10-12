# LINO HTTP Connector

This is an example to show how you can implement your own LINO Database Connector.

See project [LINO](https://github.com/CGI-FR/LINO) for more informations.

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
...
2021-10-12 14:49:45.490  INFO 2776 --- [  restartedMain] o.s.b.w.embedded.tomcat.TomcatWebServer  : Tomcat started on port(s): 8080 (http) with context path ''
2021-10-12 14:49:45.507  INFO 2776 --- [  restartedMain] com.cgi.lino.connector.Application       : Started Application in 8.814 seconds (JVM running for 9.472)
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

## License

Copyright (C) 2021 CGI France

LINO is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

LINO is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
 along with LINO.  If not, see <http://www.gnu.org/licenses/>.

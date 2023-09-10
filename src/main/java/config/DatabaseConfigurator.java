package config;

import org.apache.commons.dbcp2.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import util.Properties;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabaseConfigurator {
    private DataSource dataSource;

    private int fetchSize;

    private NamedParameterJdbcTemplate template;
    private final String password;
    private final String url;
    private final String username;
    public DatabaseConfigurator(final int fetchSize) throws IOException, SQLException {
        this.fetchSize = fetchSize;

        final java.util.Properties connectionProperties = Properties.load("connection");

        if (connectionProperties == null) {
            throw new RuntimeException("Can't create a database connection without properties");
        }

        this.password = connectionProperties.getProperty("password");
        this.url = connectionProperties.getProperty("url");
        this.username = connectionProperties.getProperty("username");

        final String databaseType = parseDatabaseType();

        if (databaseType == null) {
            throw new RuntimeException("Can't parse a supported database type out of the database url provided");
        }

        configureDataSource(databaseType);
        configureTemplate();
    }

    private void configureDataSource(String databaseType) {
        this.dataSource = generateDataSource(databaseType);
    }

    public Connection getConnection() throws SQLException {
        final Connection connection = DriverManager.getConnection(this.url, this.username, this.password);
        connection.setAutoCommit(false);
        connection.setReadOnly(true);
        return connection;
    }

    public int getFetchSize() { return fetchSize; }

    public NamedParameterJdbcTemplate getTemplate() { return this.template; }

    private void configureTemplate() {
        final JdbcTemplate template = new JdbcTemplate(this.dataSource);
        template.setFetchSize(fetchSize);

        this.template = new NamedParameterJdbcTemplate(template);
    }

    private DataSource generateDataSource(final String databaseType) {
        Objects.requireNonNull(databaseType);

        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(getDriverClassName(databaseType));
        dataSource.setPassword(this.password);
        dataSource.setUsername(this.username);
        dataSource.setUrl(this.url);

        dataSource.setDefaultAutoCommit(false);
        dataSource.setDefaultReadOnly(true);
        dataSource.setInitialSize(1);
        dataSource.setLogAbandoned(true);
        dataSource.setMaxIdle(1);
        dataSource.setMaxTotal(5);
        dataSource.setMaxWaitMillis(10000);
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedTimeout(30);
        dataSource.setTestOnBorrow(true);
        dataSource.setTestOnCreate(true);
        dataSource.setTestOnReturn(true);
        dataSource.setValidationQuery(getValidationQuery(databaseType));
        dataSource.setValidationQueryTimeout(60);

        return dataSource;
    }

    private String getDriverClassName(final String databaseType) {
        return switch (databaseType) {
            case "mysql" -> "com.mysql.cj.jdbc.Driver";
            case "postgresql" -> "org.postgresql.Driver";
            // Unsupported
            default -> null;
        };
    }

    private String getValidationQuery(final String databaseType) {
        return switch (databaseType) {
            case "mysql" -> "/* ping */ SELECT 1";
            default -> "SELECT 1"; // Matches Derby, H2, Hive, Impala, Ingres, MariaDB, SQL Server, PostgreSQL
        };
    }

    private String parseDatabaseType() {
        final Pattern pattern = Pattern.compile("jdbc:(.*?):");
        final Matcher matcher = pattern.matcher(this.url);

        if (matcher.find()) {
            return matcher.group(1);
        }

        return null;
    }
}

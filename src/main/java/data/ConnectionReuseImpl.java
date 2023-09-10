package data;

import config.DatabaseConfigurator;
import data.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapperResultSetExtractor;
import org.springframework.jdbc.core.SingleColumnRowMapper;
import org.springframework.util.ConcurrentLruCache;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConnectionReuseImpl implements SufficientDataDao {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionReuseImpl.class);
    private Connection connection;
    private final DatabaseConfigurator configurator;
    private final RecordMapper recordMapper = new RecordMapper();
    private final RecordMetaMapper metaMapper = new RecordMetaMapper();

    public ConnectionReuseImpl(final DatabaseConfigurator configurator) {
        this.configurator = configurator;
        getNewConnection();
    }

    @Override
    public DaoType getDaoType() {
        return DaoType.CONNECTION;
    }
    @Override
    public List<Long> getIds() {
        final String sql = " SELECT id" +
                           " FROM sufficient_data.sufficient_ids";

        ResultSet resultSet = null;

        try {
            final long start = System.nanoTime();
            resultSet = getStatement().executeQuery(sql);
            final List<Long> results = new RowMapperResultSetExtractor<Long>(new SingleColumnRowMapper<>())
                                            .extractData(resultSet);
            logRuntime(start, "getIds");
            return results;
        } catch (final SQLException sqle) {
            throw new RuntimeException("Failed to execute getIds with SQLException: " + sqle.getMessage());
        } finally {
            close(resultSet);
        }
    }

    @Override
    public List<String[]> getRecordsWithCreated(final Timestamp created) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE created = ?";

        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;

        try {
            final long start = System.nanoTime();
            preparedStatement.setTimestamp(1, created);
            resultSet = preparedStatement.executeQuery();
            final List<String[]> results = new RowMapperResultSetExtractor<>(this.recordMapper).extractData(resultSet);
            logRuntime(start, "getRecordsWithCreated");
            return results;
        } catch (final SQLException sqle) {
            throw new RuntimeException("Failed to execute getRecordsWithCreated with SQLException: " + sqle.getMessage());
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    @Override
    public String[] getRecordById(final Long id) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE id = ?";

        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;
        final long start = System.nanoTime();

        try {
            preparedStatement.setLong(1, id);
            resultSet = preparedStatement.executeQuery();
            final List<String[]> mappedResults = new RowMapperResultSetExtractor<>(this.recordMapper).extractData(resultSet);
            logRuntime(start, "getRecordById");
            return (mappedResults == null || mappedResults.isEmpty()) ? null : mappedResults.get(0);
        } catch (final SQLException sqle) {
            logger.error("Failed to execute getRecordById with SQLException: ", sqle);
            throw new RuntimeException(sqle);
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    @Override
    public String[] getRecordMetaById(final Long id) {
        final String sql = " SELECT other_id as id, canonical_name, description, access_restricted, last_accessed" +
                           " FROM sufficient_data.sufficient_meta" +
                           " WHERE other_id = ?";
        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;
        final long start = System.nanoTime();

        try {
            preparedStatement.setLong(1, id);
            resultSet = preparedStatement.executeQuery();
            final List<String[]> results = new RowMapperResultSetExtractor<>(this.metaMapper).extractData(resultSet);
            logRuntime(start, "getRecordMetaById");
            if (results == null || results.isEmpty()) {
                logger.debug("No meta record found");
                return null;
            }

            return results.get(0);
        } catch (final SQLException sqle) {
            logger.error("Failed to execute getRecordMetaById with SQLException: ", sqle);
            throw new RuntimeException(sqle);
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    @Override
    public List<String[]> getRecordsWithGenerated(final boolean generated) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE generated = ?";

        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;
        final long start = System.nanoTime();

        try {
            preparedStatement.setBoolean(1, generated);
            resultSet = preparedStatement.executeQuery();
            final List<String[]> results = new RowMapperResultSetExtractor<>(this.recordMapper).extractData(resultSet);
            logRuntime(start, "getRecordsWithGenerated");
            return results;
        } catch (final SQLException sqle) {
            throw new RuntimeException("Failed to execute getRecordsByGenerated with SQLException: " + sqle.getMessage());
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    @Override
    public Map<Long, String[]> getRecordsByIds(final Collection<Long> ids) {
        final String sql = String.format(
                            " SELECT id, created, value, generated" +
                            " FROM sufficient_data.sufficient_ids" +
                            " WHERE id IN (%s)",
                            ids.stream().map(id -> "?").collect(Collectors.joining(", "))
        );

        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;
        final long start = System.nanoTime();

        try {
            int currentIndex = 1;
            for (final Long id : ids) {
                preparedStatement.setLong(currentIndex, id);
                currentIndex++;
            }
            logRuntime(start, "prepare the inputs");
            resultSet = preparedStatement.executeQuery();
            final Map<Long, String[]> results = new RecordByIdExtractor().extractData(resultSet);
            logRuntime(start, "getRecordsByIds for " + ids.size() + " ids");
            return results;
        } catch (final SQLException sqle) {
            throw new RuntimeException("Failed to execute getRecordsByIds with SQLException: " + sqle.getMessage());
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    @Override
    public Map<Long, String[]> getRecordMetasByIds(Collection<Long> ids) {
        final String sqlFormat = " SELECT other_id as id, canonical_name, description, access_restricted, last_accessed" +
                                 " FROM sufficient_data.sufficient_meta" +
                                 " WHERE other_id IN (%s)";
        final String sql = String.format(
                sqlFormat,
                ids.stream().map(id -> "?").collect(Collectors.joining(","))
        );
        final PreparedStatement preparedStatement = getPreparedStatement(sql);
        ResultSet resultSet = null;
        final long start = System.nanoTime();
        try {
            int currentIndex = 1;
            for (final Long id : ids) {
                preparedStatement.setLong(currentIndex, id);
                currentIndex++;
            }
            logRuntime(start, "prepare the input");
            resultSet = preparedStatement.executeQuery();
            final Map<Long, String[]> results = new RecordMetaByIdExtractor(this.metaMapper).extractData(resultSet);
            logRuntime(start, "getRecordMetasByIds");
            return results;
        } catch (final SQLException sqle) {
            logger.error("Could not getRecordMetasByIds with SQLException: ", sqle);
            throw new RuntimeException(sqle);
        }
    }

    @Override
    public Map<Timestamp, List<String[]>> getRecordsByCreated() {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids";

        ResultSet resultSet = null;

        try {
            final long start = System.nanoTime();
            resultSet = getStatement().executeQuery(sql);
            final Map<Timestamp, List<String[]>> results = new RecordsByTimestampExtractor().extractData(resultSet);
            logRuntime(start, "getRecordsByCreated");
            return results;
        } catch (final SQLException sqle) {
            throw new RuntimeException("Failed to execute getRecordsByCreated with SQLException: " + sqle.getMessage());
        } finally {
            close(resultSet);
        }
    }

    @Override
    public Map<Boolean, List<String[]>> getRecordsByGenerated() {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids";

        ResultSet resultSet = null;
        try {
            final long start = System.nanoTime();
            resultSet = getStatement().executeQuery(sql);
            final Map<Boolean, List<String[]>> results = new RecordsByBooleanExtractor().extractData(resultSet);
            logRuntime(start, "getRecordsByGenerated");
            return results;
        } catch (final SQLException sqle) {
            System.err.println("Failed to execute getRecordsByGenerated with SQLException--");
            throw new RuntimeException(sqle);
        } finally {
            close(resultSet);
        }
    }

    private void close(final ResultSet resultSet) {
        if (resultSet == null) {
            return;
        }

        try {
            resultSet.close();
        } catch (final SQLException sqle) {
            logger.warn("Got an SQLException closing a ResultSet object, but who cares?");
        }
    }

    private void close(final Statement statement) {
        if (statement == null) {
            return;
        }

        try {
            statement.close();
        } catch (final SQLException sqle) {
            logger.warn("Got an SQLException closing a Statement object, but who cares?");
        }
    }

    private void closeConnection() {
        final long start = System.nanoTime();
        try {
            if (this.connection != null) {
                this.connection.close();
            }
            logRuntime(start, "closeConnection");
        } catch (final SQLException sqle) {
            System.err.println("Caught exception attempting to close connection, but not re-throwing");
        } finally {
            this.connection = null;
        }
    }

    private void getNewConnection() {
        logger.info("Getting a new connection from the configurator");
        final long start = System.nanoTime();
        try {
            this.connection = this.configurator.getConnection();
            logRuntime(start, "getConnection");
        } catch (final SQLException sqle) {
            logger.error("Couldn't get a new connection from the configurator with exception: ", sqle);
            throw new RuntimeException(sqle);
        }
    }

    private PreparedStatement getPreparedStatement(final String sql) {
        final long start = System.nanoTime();
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = prepareStatement(sql, 2);
            preparedStatement.setFetchSize(configurator.getFetchSize());
        } catch (final SQLException sqle) {
            logger.error("Couldn't set fetch size on the prepared statement with SQLException: ", sqle);
            throw new RuntimeException(sqle);
        }
        logRuntime(start, "getPreparedStatement");
        return preparedStatement;
    }

    private PreparedStatement prepareStatement(final String sql, final int retries) throws SQLException {
        if (retries > 0) {
            final PreparedStatement preparedStatement = getPreparedStatementLenient(sql);

            if (preparedStatement != null) {
                return preparedStatement;
            }

            return prepareStatement(sql, retries - 1);
        }

        return getPreparedStatementStrict(sql);
    }

    private PreparedStatement getPreparedStatementLenient(final String sql) {
        try {
            return this.connection.prepareStatement(sql);
        } catch (final SQLException sqle) {
            logger.error("Failed to get prepared statement with SQLException: ", sqle);
            refreshConnection();

            return null;
        }
    }

    private PreparedStatement getPreparedStatementStrict(final String sql) throws SQLException {
        return this.connection.prepareStatement(sql);
    }

    private Statement getStatement() {
        final long start = System.nanoTime();
        Statement statement = null;

        try {
            statement = getStatement(2);
            statement.setFetchSize(configurator.getFetchSize());
        } catch (final SQLException sqle) {
            logger.error("Couldn't set fetch size on statement with exception: ", sqle);
            throw new RuntimeException(sqle);
        }

        logRuntime(start, "getStatement");
        return statement;
    }

    private Statement getStatement(int retries) throws SQLException {
        if (retries > 0) {
            final Statement result = getStatementLenient();

            if (result != null) {
                return result;
            }

            return getStatement(retries - 1);
        }

        return getStatementStrict();
    }

    private Statement getStatementLenient() {
        try {
            return connection.createStatement();
        } catch (final SQLException sqle) {
            System.err.println("Couldn't get a statement from the connection");
            refreshConnection();

            return null;
        }
    }

    private Statement getStatementStrict() throws SQLException {
        return connection.createStatement();
    }

    private void logRuntime(final long start, final String methodName) {
        final long end = System.nanoTime();
        final long diff = end - start;
        if (diff >= 1_000_000) {
            logger.info("Took {} milliseconds to {}", diff / 1_000_000L, methodName);
        } else {
            logger.info("Took {} nanoseconds to {}", diff, methodName);
            logger.info("{} nanoseconds is {} of a millisecond",
                    new BigDecimal(diff),
                    new BigDecimal(diff).divide(new BigDecimal(1_000_000), 4, RoundingMode.HALF_UP)
            );
        }
    }

    private void refreshConnection() {
        if (!shouldRefreshConnection()) {
            logger.info("No reason to refresh connection at this time");
            return;
        }

        closeConnection();
        getNewConnection();
    }

    private boolean shouldRefreshConnection() {
        if (this.connection == null) {
            return true;
        }

        try {
            return this.connection.isClosed();
        } catch (final SQLException sqle) {
            logger.error("Could not check connection closed state, assuming we should refresh");
            return true;
        }
    }
}

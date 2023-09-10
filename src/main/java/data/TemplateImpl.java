package data;

import data.mapping.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TemplateImpl implements SufficientDataDao {
    private static final Logger logger = LoggerFactory.getLogger(TemplateImpl.class);
    private final NamedParameterJdbcTemplate template;
    private final RecordByIdExtractor recordByIdExtractor = new RecordByIdExtractor();

    private final RecordMapper mapper = new RecordMapper();
    private final RecordMetaMapper metaMapper = new RecordMetaMapper();
    private final RecordMetaByIdExtractor metaExtractor = new RecordMetaByIdExtractor(this.metaMapper);
    private final RecordsByBooleanExtractor byBooleanExtractor = new RecordsByBooleanExtractor();
    private final RecordsByTimestampExtractor byTimestampExtractor = new RecordsByTimestampExtractor();

    public TemplateImpl(final NamedParameterJdbcTemplate template) {
        this.template = template;
    }

    @Override
    public DaoType getDaoType() {
        return DaoType.TEMPLATE;
    }

    @Override
    public List<Long> getIds() {
        final String sql = " SELECT id" +
                           " FROM sufficient_data.sufficient_ids";

        final long start = System.nanoTime();
        final List<Long> results = template.queryForList(sql, new MapSqlParameterSource(), Long.class);
        logRuntime(start, "getIds");
        return results;
    }

    @Override
    public String[] getRecordById(Long id) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE id = :id";

        long start = System.nanoTime();
        try {
            final String[] result = template.queryForObject(sql, new MapSqlParameterSource("id", id), mapper);
            logRuntime(start, "getRecordById");
            return result;
        } catch (final IncorrectResultSizeDataAccessException dae) {
            logger.error("Couldn't materialize an object result with wrong result size: ", dae);
            throw new RuntimeException(dae);
        }
    }

    @Override
    public String[] getRecordMetaById(Long id) {
        final String sql = " SELECT other_id as id, canonical_name, description, access_restricted, last_accessed" +
                           " FROM sufficient_data.sufficient_meta" +
                           " WHERE other_id = :id";

        long start = System.nanoTime();
        try {
            final String[] result = template.queryForObject(
                    sql, new MapSqlParameterSource("id", id), this.metaMapper);
            logRuntime(start, "getRecordMetaById");
            return result;
        } catch (final EmptyResultDataAccessException dae) {
            logger.debug("No matching record found");
            return null;
        } catch (final IncorrectResultSizeDataAccessException dae) {
            logger.error("Couldn't materialize a string array with wrong result size: ", dae);
            throw new RuntimeException(dae);
        }
    }

    @Override
    public Map<Long, String[]> getRecordsByIds(final Collection<Long> ids) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE id IN (:ids)";

        final long start = System.nanoTime();
        final Map<Long, String[]> results = template.query(
                sql, new MapSqlParameterSource("ids", ids), recordByIdExtractor);
        logRuntime(start, "getRecordsByIds");
        return results;
    }

    @Override
    public Map<Long, String[]> getRecordMetasByIds(final Collection<Long> ids) {
        final String sql = " SELECT other_id as id, canonical_name, description, access_restricted, last_accessed" +
                           " FROM sufficient_data.sufficient_meta" +
                           " WHERE other_id IN (:ids)";

        final long start = System.nanoTime();
        final Map<Long, String[]> results = template.query(
                sql, new MapSqlParameterSource("ids", ids), metaExtractor);
        logRuntime(start, "getRecordMetasByIds");
        return results;
    }

    @Override
    public List<String[]> getRecordsWithCreated(final Timestamp created) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE created = :created";

        final long start = System.nanoTime();
        final SqlParameterSource params = new MapSqlParameterSource("created", created);
        final List<String[]> records = template.query(sql, params, mapper);
        logRuntime(start, "getRecordsWithCreated");
        return records;
    }

    @Override
    public List<String[]> getRecordsWithGenerated(boolean generated) {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids" +
                           " WHERE generated = :generated";

        final long start = System.nanoTime();
        final List<String[]> records = template.query(
                sql, new MapSqlParameterSource("generated", generated), mapper);
        logRuntime(start, "getRecordsWithGenerated");
        return records;
    }

    @Override
    public Map<Timestamp, List<String[]>> getRecordsByCreated() {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids";

        final long start = System.nanoTime();
        final Map<Timestamp, List<String[]>> results = template.query(
                sql, new MapSqlParameterSource(), byTimestampExtractor);
        logRuntime(start, "getRecordsByCreated");
        return results;
    }

    @Override
    public Map<Boolean, List<String[]>> getRecordsByGenerated() {
        final String sql = " SELECT id, created, value, generated" +
                           " FROM sufficient_data.sufficient_ids";

        final long start = System.nanoTime();
        final Map<Boolean, List<String[]>> results = template.query(
                sql, new MapSqlParameterSource(), byBooleanExtractor);
        logRuntime(start, "getRecordsByGenerated");
        return results;
    }

    private void logRuntime(final long start, final String methodName) {
        final long end = System.nanoTime();
        logger.info("Took {} milliseconds to {}", (end - start) / 1_000_000L, methodName);
    }
}

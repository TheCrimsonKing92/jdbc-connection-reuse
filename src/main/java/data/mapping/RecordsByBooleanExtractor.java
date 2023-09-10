package data.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class RecordsByBooleanExtractor implements ResultSetExtractor<Map<Boolean, List<String[]>>> {
    private static final Logger logger = LoggerFactory.getLogger(RecordsByBooleanExtractor.class);
    @Override
    public Map<Boolean, List<String[]>> extractData(final ResultSet rs) throws SQLException, DataAccessException {
        final long start = System.nanoTime();
        final Map<Boolean, List<String[]>> results = new RecordsByKeyExtractor<>(resultSet -> {
            try {
                return resultSet.getBoolean("generated");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, new RecordMapper()).mapRecords(rs);
        final long end = System.nanoTime();
        logger.info("Took {} milliseconds to extract records", (end - start) / 1000L);
        return results;
    }
}

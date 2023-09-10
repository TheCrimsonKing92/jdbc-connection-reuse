package data.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class RecordsByTimestampExtractor implements ResultSetExtractor<Map<Timestamp, List<String[]>>> {
    private static final Logger logger = LoggerFactory.getLogger(RecordsByTimestampExtractor.class);

    @Override
    public Map<Timestamp, List<String[]>> extractData(final ResultSet resultSet) throws SQLException, DataAccessException {
        final long start = System.nanoTime();
        final Map<Timestamp, List<String[]>> results = new RecordsByKeyExtractor<>(rs -> {
            try {
                return rs.getTimestamp("created");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, new RecordMapper()).mapRecords(resultSet);
        final long end = System.nanoTime();
        logger.info("Took {} milliseconds to extract results", (end - start) / 1000L);
        return results;
    }
}

package data.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RecordByIdExtractor implements ResultSetExtractor<Map<Long, String[]>> {
    private static final Logger logger = LoggerFactory.getLogger(RecordByIdExtractor.class);

    @Override
    public Map<Long, String[]> extractData(final ResultSet rs) throws SQLException, DataAccessException {
        final long start = System.nanoTime();
        final Map<Long, String[]> results = new HashMap<>();
        final RecordMapper mapper = new RecordMapper();

        while (rs.next()) {
            final long id = rs.getLong("id");
            results.put(id, mapper.mapRow(rs, rs.getRow()));
        }

        final long end = System.nanoTime();
        logger.info("Took {} milliseconds to extract data", (end - start) / 1_000_000L);
        return results;
    }
}

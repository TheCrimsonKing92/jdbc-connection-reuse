package data.mapping;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class RecordMetaByIdExtractor implements ResultSetExtractor<Map<Long, String[]>> {

    private final RecordMetaMapper mapper;

    public RecordMetaByIdExtractor(final RecordMetaMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Map<Long, String[]> extractData(final ResultSet rs) throws SQLException, DataAccessException {
        final Map<Long, String[]> results = new HashMap<>();

        while (rs.next()) {
            final String[] current = this.mapper.mapRow(rs, rs.getRow());
            if (current == null) {
                continue;
            }

            final Long id = Long.valueOf(current[0]);
            results.put(id, current);
        }

        return results;
    }
}

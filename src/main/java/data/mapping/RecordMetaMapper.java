package data.mapping;

import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RecordMetaMapper implements RowMapper<String[]> {
    @Override
    public String[] mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final String id = Long.toString(rs.getLong(1));
        final String name = rs.getString(2);
        final String description = rs.getString(3);
        final String accessRestricted = String.valueOf(rs.getBoolean(4));
        final String lastAccessed = rs.getTimestamp(5).toString();


        return new String[]{ id, name, description, accessRestricted, lastAccessed };
    }
}

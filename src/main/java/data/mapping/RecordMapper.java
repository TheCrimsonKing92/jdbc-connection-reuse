package data.mapping;

import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RecordMapper implements RowMapper<String[]> {
    @Override
    public String[] mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        final Timestamp ts = rs.getTimestamp("created");

        final long id = rs.getLong("id");
        final BigDecimal value = rs.getBigDecimal("value");
        final boolean generated = rs.getBoolean("generated");

        return new String[]{ Long.toString(id), ts.toString(), value.toString(), String.valueOf(generated) };
    }
}

package data.mapping;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RecordsByKeyExtractor<T> {
    private final Function<ResultSet, T> keyFunction;
    private final RecordMapper mapper;

    public RecordsByKeyExtractor(final Function<ResultSet, T> keyFunction, final RecordMapper mapper) {
        this.keyFunction = keyFunction;
        this.mapper = mapper;
    }

    public Map<T, List<String[]>> mapRecords(final ResultSet resultSet) throws SQLException {
        final Map<T, List<String[]>> results = new HashMap<>();

        while (resultSet.next()) {
            final T key = keyFunction.apply(resultSet);

            results.putIfAbsent(key, new ArrayList<>());

            results.get(key).add(this.mapper.mapRow(resultSet, resultSet.getRow()));
        }

        return results;
    }
}

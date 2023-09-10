package data;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface SufficientDataDao {
    public DaoType getDaoType();
    public List<Long> getIds();
    public String[] getRecordById(Long id);
    public String[] getRecordMetaById(Long id);
    public Map<Long, String[]> getRecordsByIds(final Collection<Long> ids);
    public Map<Long, String[]> getRecordMetasByIds(final Collection<Long> ids);
    public Map<Timestamp, List<String[]>> getRecordsByCreated();
    public Map<Boolean, List<String[]>> getRecordsByGenerated();
    public List<String[]> getRecordsWithCreated(final Timestamp created);
    public List<String[]> getRecordsWithGenerated(final boolean generated);
}

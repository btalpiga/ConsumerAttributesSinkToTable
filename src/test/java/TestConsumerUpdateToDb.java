import com.nyble.main.App;
import com.nyble.main.RecordProcessorImpl;
import com.nyble.models.consumer.CAttribute;
import com.nyble.models.consumer.Consumer;
import com.nyble.util.DBUtil;
import org.junit.Before;
import org.junit.jupiter.api.BeforeEach;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DBUtil.class, App.class})
public class TestConsumerUpdateToDb {
    final static String queryStr = "SELECT payload from consumers where system_id = ? and consumer_id = ?";
    final static String updateStr = "INSERT INTO consumers (system_id, consumer_id, payload, updated_at) values (?, ?, ?, now()) \n" +
            "on conflict on constraint consumers_pk do update set payload=excluded.payload, updated_at = now()";

    @Mock
    DBUtil instance;

    @Mock
    Connection connection;

    @Mock
    PreparedStatement queryConsumer;

    @Mock
    PreparedStatement updateConsumer;

    @Mock
    Statement uniqueAttrsInsert;

    RecordProcessorImpl recordProcessor;

    @Before
    public void initTest() throws Exception {
        PowerMockito.mockStatic(DBUtil.class);
        BDDMockito.given(DBUtil.getInstance()).willReturn(instance);
        when(instance.getConnection(anyString())).thenReturn(connection);
        when(connection.prepareStatement(queryStr)).thenReturn(queryConsumer);
        when(connection.prepareStatement(updateStr)).thenReturn(updateConsumer);
        when(queryConsumer.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(uniqueAttrsInsert);


        PowerMockito.mockStatic(App.class);
        PowerMockito.doNothing().when(App.class, "sendToTopic", anyInt(), anyInt(), any(), anyString(), anyString(), anyString());

        recordProcessor = new RecordProcessorImpl();
    }


    @Test
    public void test_consumerExists() throws SQLException {
        String now = System.currentTimeMillis()+"";
        Consumer existingConsumer = new Consumer();
        existingConsumer.setProperty("o2oInCnt", new CAttribute("10", now));

        ResultSet mockRs = Mockito.mock(ResultSet.class);
        when(mockRs.next()).thenReturn(true).thenReturn(false);
        when(mockRs.getString(1)).thenReturn(existingConsumer.toJson());
        when(queryConsumer.executeQuery()).thenReturn(mockRs);

        Map<String, CAttribute> newAttributes = new HashMap<>();
        newAttributes.put("webInCnt", new CAttribute("2", now));
        recordProcessor.processConsumer(queryConsumer, updateConsumer, 1, 1, newAttributes);

    }

    @Test
    public void test_consumerExists_uniqueFieldUpdated() throws SQLException {
        String now = System.currentTimeMillis()+"";
        Consumer existingConsumer = new Consumer();
        existingConsumer.setProperty("o2oInCnt", new CAttribute("10", now));

        ResultSet mockRs = Mockito.mock(ResultSet.class);
        when(mockRs.next()).thenReturn(true).thenReturn(false);
        when(mockRs.getString(1)).thenReturn(existingConsumer.toJson());
        when(queryConsumer.executeQuery()).thenReturn(mockRs);

        Map<String, CAttribute> newAttributes = new HashMap<>();
        newAttributes.put("email", new CAttribute("aaaaaaa", now));
        newAttributes.put("o2oInCnt", new CAttribute("+3", now));
        newAttributes.put("webInCnt", new CAttribute("-1", now));
        recordProcessor.processConsumer(queryConsumer, updateConsumer, 1, 1, newAttributes);
    }

    public void test_consumerNotExists(){

    }

    public void test_consumerNotExists_uniqueFieldUpdated(){

    }
}

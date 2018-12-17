package java.test;

import com.yanghui.elephant.client.exception.MQClientException;
import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

public class ApplicationTest {

    @Test
    @Transactional
    @Rollback(value = false)
    public void applyAppTest() throws MQClientException {

    }
}

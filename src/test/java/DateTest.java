import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author soberhjj  2020/4/23 - 13:40
 */
public class DateTest {
    public static void main(String[] args) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        System.out.println(sdf.format(new Date()));
    }
}

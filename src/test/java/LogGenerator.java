import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author soberhjj  2020/4/22 - 10:52
 */
public class LogGenerator {
    public static void main(String[] args) {
        String category []={"donghua","music",
                "dance","technology","life",
                "fasion","ent","cinema",
                "anime","guochuang","game",
                "digital"
        };
        Random random=new Random();
        int num=category.length;
        File file=new File("/opt/soft/logdata");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            OutputStream os=new FileOutputStream(file);
            while (true) {
                String str=sdf.format(new Date())+"\t"+category[random.nextInt(num)]+"\n";
                os.write(str.getBytes());
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

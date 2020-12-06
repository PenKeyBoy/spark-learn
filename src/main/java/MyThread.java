/**
 * Created by 金伟华 on 2018/1/14.
 */
import java.lang.Runnable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class MyThread{
    String threadName;
    int delay;
    public MyThread(String threadName,int delay){
        this.threadName = threadName;
        this.delay =delay;

    }
    public static void main(String[] args) throws InterruptedException{
        Boolean exitFlag = false;
        int counter =5;
        MyThread thread1 = new MyThread("thread-1",2);
        MyThread thread2 = new MyThread("thread-2",1);
        myrun threadTask1 = thread1.new myrun();
        myrun threadTask2 = thread2.new myrun();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        while(!exitFlag && counter>0){
            try{
                executorService.submit(threadTask1);
                //executorService.submit(threadTask2);
                Thread.sleep(thread1.delay);
                counter-=1;
            }finally {
                System.out.println("当前计数："+counter+"\n状态为："+exitFlag);

            }
        }
        exitFlag = true;
        //executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        executorService.shutdown();
        System.out.println("所有线程执行完毕"+"\n--当前状态为："+exitFlag);
    }
    class myrun implements Runnable{

        public void run(){
            Date date = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
            String nowDate = sdf.format(date);
            System.out.println("当前日期为："+nowDate);
        }
    }
}

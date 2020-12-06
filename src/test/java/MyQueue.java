/**
 * Created by 金伟华 on 2018/1/7.
 */
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class MyQueue {
    /*
    public static void main(String[] args){
        Queue<Integer> queue = new LinkedList<Integer>();
        queue.add(1);queue.add(2);queue.offer(3);
        for(Integer ele : queue ){
            System.out.println("队列元素依次：" + ele);
        }
        //peek和poll的区别元素出队列，队列元素随之减少
        int topOne = queue.poll();
        //元素副本出队列，即队列里的元素个数并没改变
        int topOne1 = queue.peek();
        System.out.println("队列出列：" + topOne);
        for(Integer ele : queue ){
            System.out.println("现在队列元素依次：" + ele);
        }
    }
    */
    int queueSize = 0;
    int threadWritingBackNum = 0;
    Queue<Dataset> queue = new LinkedList<Dataset>();
    private void enque(Dataset<Row> df){
        queue.add(df);
        queueSize+=1;

    }
    private Tuple2<Dataset<Row>,Integer> dequeue(){
        Dataset<Row> df = queue.peek();
        queueSize-=1;
        threadWritingBackNum+=1;
        //Triplet<>
        return Tuple2.apply(df, threadWritingBackNum);
    }
    private int Size(){
        return queueSize;
    }
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("TESTQUEUE").master("local[2]").getOrCreate();
        List<Row> dt = Arrays.asList(
                RowFactory.create(0L, Vectors.sparse(5,new int[]{0,2,3},new double[]{1.0,1.0,2.0})),
                RowFactory.create(1L, Vectors.dense(new double[]{1.0,1.0,2.0,3.0,4.0}))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("ID", DataTypes.LongType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(dt, schema);
        MyQueue myQueue = new MyQueue();
        myQueue.enque(df);
        System.out.println("the new size of queue: " + myQueue.Size());
        //此处没有运用泛型，因此返回的类型是java.lang.Object,后面需要强制类型转换才可以输出
        Tuple2 res = myQueue.dequeue();
        Dataset<Row> resDF = (Dataset<Row>) res._1();
        System.out.println("输出队列元组结果的第一个元素：");
        resDF.show();
        int writingBackThreadNum = (Integer) res._2();
        System.out.println("output the second element of queue: " + writingBackThreadNum);
        System.out.println("the size of new queue: " + myQueue.Size());
        //用泛型
        Tuple2<Dataset<Row>,Integer> res1 = myQueue.dequeue();
        Dataset<Row> resDF1 = res1._1;
        System.out.println("输出队列元组结果的第一个元素：");
        resDF1.show();
        int writingBackThreadNum1 = res1._2;
        System.out.println("output the second element of queue: " + writingBackThreadNum1);
    }

}

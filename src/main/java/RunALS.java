import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by jinwe_000 on 2018/3/5.
 */
public class RunALS {
    public static class MyRating implements Serializable {
        private int userID;
        private int itemID;
        private double rating;
        private long timestamp;
        public MyRating (int userID, int itemID, double rating, long timestamp){
            this.userID = userID;
            this.itemID = itemID;
            this.rating = rating;
            this.timestamp = timestamp;
        }
        public MyRating(){}
        public Integer getUserID (){
            return userID;
        }
        public int getItemID (){
            return itemID;
        }
        public double getRating (){
            return rating;
        }
        public long getTimestamp (){
            return timestamp;
        }
        public static MyRating parseDating(String item){
            String[]  field = item.split("::");
            if(field.length != 4){
                throw new IllegalArgumentException("the item should contains four fields");
            }
            int userID = Integer.parseInt(field[0]);
            int itemID = Integer.parseInt(field[1]);
            double rating = Double.parseDouble(field[2]);
            long timestamp = Long.parseLong(field[3]);
            return new MyRating(userID, itemID, rating, timestamp);
        }

    }
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("alsModel").master("local[2]").getOrCreate();
        String path = "F:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\als\\sample_movielens_ratings.txt";
        JavaRDD<MyRating> ratingRDD = spark.read().textFile(path).javaRDD().map(new Function<String, MyRating>() {
            public MyRating call(String s) throws Exception {
                return MyRating.parseDating(s);
            }
        });
        Dataset<Row> movieDT = spark.createDataFrame(ratingRDD,MyRating.class);
        Dataset<Row>[] trainTestMovie = movieDT.randomSplit(new double[]{0.7,0.3});
        Dataset<Row> trainMovie = trainTestMovie[0];
        Dataset<Row> testMovie = trainTestMovie[1];
        ALS als = new ALS().setMaxIter(10).setRegParam(0.1).
                setUserCol("userID").setItemCol("itemID").setRatingCol("rating");
        ALSModel alsModel = als.fit(trainMovie);
        //通过测试集计算准确率，在此之前需对NAN值进行剔除操作（冷处理策略）
        Dataset<Row> prediction = alsModel.transform(testMovie);
        RegressionEvaluator evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(prediction);
        System.out.println("协同过滤的测试集的均方误差计算结果："+rmse);
        System.out.println("默认秩的结果："+alsModel.rank());
        //列出每部电影推荐结果前10的userID
        
        Dataset<Row> userRec = alsModel.userFactors();
        userRec.show(false);
    }
}
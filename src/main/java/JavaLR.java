import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import scala.Tuple2;

/**
 * Created by 金伟华 on 2018/1/22.
 */
public class JavaLR {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("myJavaLineaRgression").setMaster("local[3]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        String path = "F:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\ridge-data\\lpsa.data";
        JavaRDD<String> textRDD = jsc.textFile(path,20);
        final JavaRDD<LabeledPoint> javaRDD = textRDD.map(new Function<String, LabeledPoint>() {
            public LabeledPoint call(String s) throws Exception {
                String[] row = s.split(",");
                String[] features = row[1].split(" ");
                double[] vectorFeat = new double[features.length];
                for(int i=0;i<features.length-1;i++){
                    vectorFeat[i] = Double.parseDouble(features[i]);

                }
                return new LabeledPoint(Double.parseDouble(row[0]),Vectors.dense(vectorFeat));
            }
        });
        javaRDD.cache();
        int numIterations = 100;
        double stepSize = 1e-4;
        final LinearRegressionModel LRM = LinearRegressionWithSGD.train(JavaRDD.toRDD(javaRDD),numIterations,stepSize);
        JavaPairRDD<Double,Double> valueAndPred = javaRDD.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
            public Tuple2<Double, Double> call(LabeledPoint labeledPoint) throws Exception {
                return new Tuple2<Double, Double>(LRM.predict(labeledPoint.features()),labeledPoint.label());
            }
        });
        double mse = valueAndPred.mapToDouble(new DoubleFunction<Tuple2<Double,Double>>() {
            public double call(Tuple2<Double,Double> tuple2) throws Exception {
                double desc = tuple2._1() - tuple2._2();
                return desc*desc;
            }
        }).mean();

        System.out.println("平均误差："+mse);
        LRM.save(jsc.sc(),"target\\tmp\\javaLinearRegressionModel");
        LinearRegressionModel sameLRM = LinearRegressionModel.load(jsc.sc(),"target\\tmp\\javaLinearRegressionModel");
        jsc.close();
    }
}

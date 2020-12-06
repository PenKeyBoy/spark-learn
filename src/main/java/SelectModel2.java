import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SystemClock;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.util.Arrays;

import java.util.List;

/**
 * Created by jinwe_000 on 2018/1/27.
 */
public class SelectModel2 {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    public static void main(String[] args){
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("selectModel").master("local[2]").getOrCreate();
        List<Row> dtList = Arrays.asList(
                RowFactory.create(0L,"spark good",1.0),
                RowFactory.create(1L, "b d", 0.0),
                RowFactory.create(2L,"spark f g h", 1.0),
                RowFactory.create(3L, "hadoop mapreduce", 0.0),
                RowFactory.create(4L, "b spark who", 1.0),
                RowFactory.create(5L, "g d a y", 0.0),
                RowFactory.create(6L, "spark fly", 1.0),
                RowFactory.create(7L, "was mapreduce", 0.0),
                RowFactory.create(8L, "e spark program", 1.0),
                RowFactory.create(9L, "a e c l", 0.0),
                RowFactory.create(10L, "spark compile", 1.0),
                RowFactory.create(11L, "hadoop software", 0.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType,false, Metadata.empty()),
                new StructField("sentence",DataTypes.StringType,false,Metadata.empty()),
                new StructField("label",DataTypes.DoubleType,false,Metadata.empty())
        });
        Dataset<Row> dt = spark.createDataFrame(dtList,schema);
        Tokenizer token = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        HashingTF hash = new HashingTF().setInputCol(token.getOutputCol()).setOutputCol("features").setNumFeatures(1000);
        LogisticRegression logR = new LogisticRegression().setMaxIter(10).setRegParam(0.1).setElasticNetParam(0.3);
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{token,hash,logR});
        ParamGridBuilder paramGridBuilder = new ParamGridBuilder().addGrid(hash.numFeatures(),new int[]{100,1000,10000}).addGrid(logR.regParam(),
                new double[]{0.1,0.01,0.001});
        ParamMap[] paramMap = paramGridBuilder.build();
        CrossValidator cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator()).setEstimatorParamMaps(paramMap).setNumFolds(2);
        CrossValidatorModel cvModel = cv.fit(dt);
        //测试集tranform
        StructType testSchema = new StructType(new StructField[]{
                new StructField("id",DataTypes.LongType,false,Metadata.empty()),
                new StructField("sentence",DataTypes.StringType,false,Metadata.empty())
        });
        Dataset<Row> test = spark.createDataFrame(Arrays.asList(
                RowFactory.create(4L, "spark i j k"),
                RowFactory.create(5L, "l m n"),
                RowFactory.create(6L, "mapreduce spark"),
                RowFactory.create(7L, "apache hadoop")
        ), testSchema);
        Dataset<Row> prediction = cvModel.transform(test);

        for(int i=0;i<cvModel.getEstimatorParamMaps().length;i++){
            System.out.println("采用cv验证result,param foreach is:"+cvModel.getEstimatorParamMaps()[i]);
        }

        for(Row i:prediction.select("id","sentence","probability","prediction").collectAsList()){
            System.out.println("[id is"+i.get(0)+" and sentence perform"+i.get(1)+"]--->"+"probability is:"+i.get(2)+", prediction is:"+i.get(3));
        }
        Model bestModel = cvModel.bestModel();

        System.out.println("the best model params is : "+bestModel.explainParams());
        Dataset<Row>[] randDS = dt.randomSplit(new double[]{0.7,0.3});
        Dataset<Row> randTrain = randDS[0];Dataset<Row> randTest = randDS[1];
        CrossValidatorModel cvModelTrain = cv.fit(randTrain);
        Dataset<Row> testPrediction = cvModelTrain.transform(randTest);
        BinaryClassificationEvaluator classEva = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction");
        //default metricName:areaUnderROC
        System.out.println("test dataset result:");
        for(Row i:testPrediction.select("id","sentence","probability","prediction").collectAsList()){
            System.out.println("[id is"+i.get(0)+" and sentence perform"+i.get(1)+"]--->"+"probability is:"+i.get(2)+", prediction is:"+i.get(3));
        }
        double predRes = classEva.evaluate(testPrediction);
        System.out.println("evaluator result about metricName: "+classEva.getMetricName());
        System.out.println("predict result: "+predRes);
    }
}
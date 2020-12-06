import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
/**
 * Created by 金伟华 on 2018/1/7.
 */
public class ClassifyModelTest {
   public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("ClassifyModel").master("local[2]").getOrCreate();
        Dataset<Row> dt = spark.read().format("libsvm").load("E:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt").cache();
        //处理数据集，拆分训练集和测试集
        Dataset<Row>[] dtSplit = dt.randomSplit(new double[]{0.7,0.3});
        Dataset<Row> trainDT = dtSplit[0];Dataset<Row> testDT = dtSplit[1];
        //处理特征，字符型索引转换
        StringIndexerModel stringIndexerModel = new StringIndexer().setInputCol("label").setOutputCol("indexLabel").fit(dt);
        VectorIndexerModel vectorIndexerModel = new VectorIndexer().setInputCol("features").
                setOutputCol("featuresIndex").setMaxCategories(4).fit(dt);
       //类型不是分类树model(transformer)，而是分类器的estimator
        DecisionTreeClassifier dtcf = new DecisionTreeClassifier().setMaxDepth(5).
                setFeaturesCol("featuresIndex").setLabelCol("indexLabel");
        //逆转，将向量索引转化为字符型
        IndexToString labelConvert = new IndexToString().setInputCol("prediction").
                setOutputCol("predictionLabel").setLabels(stringIndexerModel.labels());
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                stringIndexerModel,vectorIndexerModel,dtcf,labelConvert
        });
        PipelineModel pipelineModel = pipeline.fit(trainDT);
        Dataset<Row> cftRes = pipelineModel.transform(testDT);
        System.out.println("采用分类树的结果：");
        cftRes.show(5);
        //cftRes.select("features","label","predictionLabel").show(6);
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexLabel").
                setPredictionCol("prediction").setMetricName("accuracy");
        double accuracy = evaluator.evaluate(cftRes);
        System.out.println("预测准确率为："+accuracy);
        DecisionTreeClassificationModel treeModel = (DecisionTreeClassificationModel) pipelineModel.stages()[2];
        System.out.println("树模型的结果是：\n"+treeModel.toDebugString());
    }
}

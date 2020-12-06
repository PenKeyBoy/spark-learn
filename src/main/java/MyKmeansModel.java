import org.apache.commons.math3.ml.clustering.evaluation.ClusterEvaluator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.evaluation.Evaluator;


/**
 * Created by jinwe_000 on 2018/3/5.
 */
public class MyKmeansModel {
    static String path1 = "F:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\sample_kmeans_data.txt";
    static String path2 = "F:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\sample_lda_libsvm_data.txt";
    public static void main(String[] args){
        //define logLevel
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder().appName("myKmeans").master("local[2]").getOrCreate();
        Dataset<Row> kmean_dt1 = spark.read().format("libsvm").load(path1);

        System.out.println("数据集1的样式：");
        kmean_dt1.show(false);
        String[] featureAssemble = {"feature1","feature2","featur3"};

        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        KMeansModel kmeansModel = kmeans.fit(kmean_dt1);
        Dataset<Row> predictions = kmeansModel.transform(kmean_dt1);
        System.out.println("kmeans 的结果: ");
        predictions.show(6);

        ClusteringSummary summary = new ClusteringSummary(predictions,"prediction","features",2);
        Evaluator evaluator = new Evaluator() {
            @Override
            public Evaluator copy(ParamMap extra) {
                return null;
            }

            @Override
            public double evaluate(Dataset<?> dataset) {
                return 0;
            }

            public String uid() {
                return null;
            }
        };
        Vector[] centers = kmeansModel.clusterCenters();
        System.out.println("cluster center: ");
        for(Vector center: centers){
            System.out.println(center);
        }

        Dataset<Row> lda_dt = spark.read().format("libsvm").load(path2);
        LDA lda = new LDA().setK(10).setSeed(11L);
        LDAModel ldaModel = lda.fit(lda_dt);
        Dataset<Row> ldaPredictions = ldaModel.transform(lda_dt);

        System.out.println("ldaModel predict dataset: ");
        ldaPredictions.show(6);
        double ll = ldaModel.logLikelihood(lda_dt);
        double lp = ldaModel.logPerplexity(lda_dt);
        System.out.println("ldaModel logLikelihood result: "+ll+"\nldaModel logPerplexity result: "+lp);

        BisectingKMeans bkm = new BisectingKMeans().setFeaturesCol("features").setK(10).setSeed(2L);
        BisectingKMeansModel bkmModel = bkm.fit(kmean_dt1);
        double cost = bkmModel.computeCost(kmean_dt1);
        System.out.println("the means of square error sum is: "+cost);

        Vector[] vectors = bkmModel.clusterCenters();
        System.out.println("cluster center vetor is: ");
        for(Vector vector:vectors){
            System.out.println(vector);
        }

        GaussianMixture gm = new GaussianMixture().setK(2).setSeed(11);
        GaussianMixtureModel gmm = gm.fit(kmean_dt1);
        System.out.println("gmm的结果是: ");
        gmm.transform(kmean_dt1);
        for(int i=0; i<gmm.getK(); i++){
            System.out.printf("第%d个中心的聚类信息:\nweight=%f\nmean=%s\ncov矩阵为:\n%s\n\n",i,gmm.weights()[i],gmm.gaussians()[i].mean(),gmm.gaussians()[i].cov());
        }
    }
}
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import static  org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;

/**
 * Created by 金伟华 on 2018/1/2.
 */
public class BucketLSH {
    public static  void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("bucketHashLSH").master("local[2]").getOrCreate();
        List<Row> listA = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0,1.0)),
                RowFactory.create(1, Vectors.dense(1.0, -1.0)),
                RowFactory.create(2, Vectors.dense(-1.0, -1.0)),
                RowFactory.create(3, Vectors.dense(-1.0, 1.0))
        );
        List<Row> listB = Arrays.asList(
                RowFactory.create(4, Vectors.dense(1.0, 0.0)),
                RowFactory.create(5, Vectors.dense(-1.0, 0.0)),
                RowFactory.create(6, Vectors.dense(0.0, 1.0)),
                RowFactory.create(7, Vectors.dense(0.0, -1.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType,false, Metadata.empty()),
                new StructField("features",new VectorUDT(),false,Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(listA,schema);
        Dataset<Row> dfB = spark.createDataFrame(listB,schema);
        BucketedRandomProjectionLSH brpLSH = new BucketedRandomProjectionLSH().setBucketLength(2).setNumHashTables(3).setInputCol("features").setOutputCol("hash");
        BucketedRandomProjectionLSHModel brpA = brpLSH.fit(dfA);
        BucketedRandomProjectionLSHModel brpB = brpLSH.fit(dfB);
        //transform A AND B
        Dataset<Row> transformA = brpA.transform(dfA);
        Dataset<Row> transformB = brpB.transform(dfB);
        System.out.println("数据集A进行hash转换后的特征：");
        transformA.show(false);
        System.out.println("数据集B进行hash转换后的特征：");
        transformB.show(false);
        //join two dataset A and B,select the limit distance factor records
        System.out.println("----计算联结后的结果----\n");
        System.out.println("数据集A的模型拟合后，联结数据集A和B联结，取欧式距离为1.5，选取的特征记录：");
        //通过col方法来传入列名
        brpA.approxSimilarityJoin(dfA,dfB,1.5,"EuclideanDistance").select(
                col("datasetA.id").alias("ida"),col("datasetB.id").alias("idb"),col("EuclideanDistance")
        ).show(false);
        //用B拟合的模型,结果一样
        System.out.println("-----------");
        System.out.println("数据集B的模型拟合后，联结数据集A和B联结，取欧式距离为1.5，选取的特征记录：");
        //通过col方法来传入列名
        brpB.approxSimilarityJoin(dfA,dfB,1.5,"EuclideanDistance").select(
                col("datasetA.id").alias("ida"),col("datasetB.id").alias("idb"),col("EuclideanDistance")
        ).show(false);
        System.out.println("转变后的数据集A和B join，取欧式距离为1.5，选取的特征记录：");
        brpA.approxSimilarityJoin(transformA,transformB,1.5,"EuclideanDistance").show();
        //通过基向量（参照），计算与基向量距离，选取记录小于阈值的记录
        Vector key = Vectors.dense(1.0,0);
        System.out.println("----计算与基向量的距离-----\n");
        System.out.println("数据集A与key的距离最近的前2条记录：");
        brpA.approxNearestNeighbors(dfA,key,2,"EuclideanDistance").show();
        System.out.println("更换拟合模型：");
        brpB.approxNearestNeighbors(dfA,key,2,"EuclideanDistance").show();

        System.out.println("-----------------");
        System.out.println("数据集B与key的距离最近的前2条记录：");
        brpB.approxNearestNeighbors(dfB,key,2,"EuclideanDistance").show();
        System.out.println("更换拟合模型：");
        brpA.approxNearestNeighbors(dfA,key,2,"EuclideanDistance").show();
    }
}

import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.ListFilesCommand;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Created by 金伟华 on 2018/1/2.
 */
public class MinLSH {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("minHashLSH").master("local[2]").getOrCreate();
        List<Row> listC = Arrays.asList(
                RowFactory.create(0l, Vectors.sparse(6,new int[]{0,1,2},new double[]{1.0,1.0,1.0})),
                RowFactory.create(1l,Vectors.sparse(6,new int[]{2,3,4},new double[]{1.0,1.0,1.0})),
                RowFactory.create(2l,Vectors.sparse(6,new int[]{0,2,4},new double[]{1.0,1.0,1.0}))

        );
        List<Row> listD = Arrays.asList(
                RowFactory.create(0l, Vectors.sparse(6, new int[]{1, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(1l, Vectors.sparse(6, new int[]{2, 3, 5}, new double[]{1.0, 1.0, 1.0})),
                RowFactory.create(2l, Vectors.sparse(6, new int[]{1, 2, 4}, new double[]{1.0, 1.0, 1.0}))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfC = spark.createDataFrame(listC, schema);
        Dataset<Row> dfD = spark.createDataFrame(listD, schema);
        int[] indice = {1,3};
        double[] values = {1.0,1.0};
        Vector key = Vectors.sparse(6,indice,values);
        MinHashLSH mhLSH = new MinHashLSH().setNumHashTables(3).setInputCol("features").setOutputCol("minHASH");
        MinHashLSHModel mhLSHModelA = mhLSH.fit(dfC);
        MinHashLSHModel mhLSHModelB = mhLSH.fit(dfD);
        System.out.println("数据集A转换后的结果");
        mhLSHModelA.transform(dfC).show(false);

        System.out.println("Approximately joining dfA and dfB on Jaccard distance smaller than 0.6:");
        mhLSHModelA.approxSimilarityJoin(dfC, dfD, 0.6, "JaccardDistance").show();
    }
}

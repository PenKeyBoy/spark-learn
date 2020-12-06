import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;


import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;

import static sun.misc.Version.println;

/**
 * Created by 金伟华 on 2017/12/24.
 */
public class OneHot {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("OneHotEncode").master("local[2]").getOrCreate();
        List<Row> data = Arrays.asList(
                RowFactory.create(1,"a"),RowFactory.create(2,"d"),RowFactory.create(3,"c"),
                RowFactory.create(4,"b"),RowFactory.create(5,"a"),RowFactory.create(6,"a"),
                RowFactory.create(7,"e"),RowFactory.create(8,"d"),RowFactory.create(9,"c")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType,false, Metadata.empty()),
                new StructField("category", DataTypes.StringType,false,Metadata.empty())
        });
        Dataset<Row> ds = spark.createDataFrame(data,schema);
        //StringIndexer index = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex");
        StringIndexerModel indexModel = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(ds);
        Dataset<Row> indexDs = indexModel.transform(ds);
        System.out.println("输出指数转换的数据框结果：");
        indexDs.show(false);
        System.out.println("indexer input colname: "+indexModel.getInputCol()+"\nindexer output colname: "+indexModel.getOutputCol());
        StructField inputColSchema = indexDs.schema().apply(indexModel.getOutputCol());
        System.out.println("the stringIndexer will be used to store labels in metadata: "+
                org.apache.spark.ml.attribute.Attribute.fromStructField(inputColSchema).toString()+"\n");
        //convert the indexRes into origin
        IndexToString convert = new IndexToString().setInputCol("categoryIndex").setOutputCol("categoryOrigin");
        Dataset<Row> convertDs = convert.transform(indexDs);
        System.out.println("逆转换的结果：");
        convertDs.show();
        //OneHot
        OneHotEncoder oneHotEncoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVector");
        Dataset<Row> oneHotDs = oneHotEncoder.transform(indexDs);
        System.out.println("独热编码的转换结果：");
        oneHotDs.show(false);   //输出结果为自由度（策略数-1），策略数index（按？升序）,double值(1.0：表示选取该策略)
        //vectorIndex
        String vectorPath = "E:\\备份software\\spark-2.0.1-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt";
        Dataset<Row> vectorDs = spark.read().format("libsvm").load(vectorPath);
        System.out.println("original数据集：");
        vectorDs.show(false);
        VectorIndexer vectorIndexer = new VectorIndexer().setInputCol("features").setOutputCol("VectorIndexFeature").setMaxCategories(5);
        VectorIndexerModel vectorIndexerModel = vectorIndexer.fit(vectorDs);
        Dataset<Row> vectorIndexDF = vectorIndexerModel.transform(vectorDs);
        System.out.println("vectorIndexDF的结果数据集：");
        vectorIndexDF.show(6);
    }
}

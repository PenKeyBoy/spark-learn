import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * Created by jinwe_000 on 2018/2/27.
 */
public class FPPredict {
    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("fp").master("local[2]").getOrCreate();
        List<Row> dtList = Arrays.asList(
                RowFactory.create(Arrays.asList("1 2 4".split(" "))),
                RowFactory.create(Arrays.asList("1 3 4 5".split(" "))),
                RowFactory.create(Arrays.asList("2 3 6 4".split(" ")))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("item",DataTypes.StringType,false, Metadata.empty())
        });
        Dataset<Row> dt = spark.createDataFrame(dtList,schema);


    }
}
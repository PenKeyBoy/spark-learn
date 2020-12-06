import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import java.io.IOException;

import static java.lang.System.out;

/**
 * Created by jinwe_000 on 2018/3/20.
 */
public class HBaseRowCounter {
    private static final String Name = "rowcounter";
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable,Result>{
        /*counter the actual row by define enum*/
        static enum Counter{
            ROWS;
        }
        @Override
        public void map(ImmutableBytesWritable row,Result result,Context context) throws IOException {
            for(KeyValue value:result.list()){
                if(value.getValue().length > 0){
                    context.getCounter(Counter.ROWS).increment(1L);
                    break;
                }
            }

        }
        /** define field function: private + return type(void/else) + method name(createSubmittableJob)
          * @param conf
          * @param args
          * @return Job type
          **/
        private static Job createSubmittableJob(Configuration conf,String[] args) throws IOException{
            String tableName = args[0];
            Job job = new Job(conf,Name + "_" + tableName);
            StringBuilder stringBuilder = new StringBuilder();
            final int columnOffset = 1;
            for(int i = columnOffset; i < args.length; i++){
                if(i > columnOffset){
                    stringBuilder.append(" ");
                }
                stringBuilder.append(args[i]);
            }
            //scan and complete column
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            if(stringBuilder.length() > 0){
                for(String columnName:stringBuilder.toString().split(" ")){
                    String[] fields = columnName.split(":");
                    if(fields.length == 1){
                        scan.addFamily(Bytes.toBytes(fields[0]));
                    }else{
                        scan.addColumn(Bytes.toBytes(fields[0]),Bytes.toBytes(fields[1]));
                    }
                }
            }
            //second argument is tableName
            job.setOutputFormatClass(NullOutputFormat.class);
            TableMapReduceUtil.initTableMapperJob(tableName,scan,RowCounterMapper.class,ImmutableBytesWritable.class,Result.class,job);
            job.setNumReduceTasks(0);
            return job;
        }
        //define the main method
        public static void main(String[] args) throws Exception{
            //set log level
            Logger.getLogger("org").setLevel(Level.ERROR);
            Logger.getLogger("akka").setLevel(Level.ERROR);
            Configuration conf = HBaseConfiguration.create();
            String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
            if(otherArgs.length < 1){
                out.println("ERROR: wrong number of parameters: "+otherArgs.length);
                out.println("Usage: HBaseRowCounter<tablename>[<column1>,<column2>,<column3>...]");
                System.exit(-1);
            }
            Job job = createSubmittableJob(conf,otherArgs);
            System.exit(job.waitForCompletion(true)? 0:1);
        }

    }
}
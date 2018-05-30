package javacode;

/**
 * Created by lenovo on 2017/12/29.
 */

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import java.util.HashMap;
import java.util.Properties;
import com.google.gson.JsonArray;
import com.google.gson.JsonIOException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;


public class KafkaConsumer {
    //private Consumer consumer;
    private static final String TOPIC = "TP_C017_E01";
    private static final int THREAD_AMOUNT = 1;
    private static String brokerlist = "10.162.2.83:2181,10.162.2.84:2181,10.162.2.85:2181,10.162.2.83:9092,10.162.2.84:9092,10.162.2.85:9092,10.162.2.86:9092,10.162.2.87:9092,10.162.20.27:18284";

    private Properties getProperties()
    {
        Properties prop = new Properties();
        //prop.put("zookeeper_connect","cdh1,cdh3,cdh4");
        prop.put("group.id","1229_lyh");
        //prop.put("zookeeper.session.timeout.ms","");
        //prop.put("auto.commit.interval.ms","");
        prop.put("auto.offset.reset","smallest");
        //prop.put("rebalance.max.retries","");
        //prop.put("rebalance.backoff.ms","");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("metadata.broker.list", brokerlist);

        return prop;
    }

    private int consume() throws IOException
    {
        Properties props = getProperties();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // 一个线程
        topicCountMap.put(TOPIC,new Integer(1));
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        Map<String, List<KafkaStream<byte[], byte[]>>> msgStreams = consumer.createMessageStreams(topicCountMap );
        List<KafkaStream<byte[], byte[]>> msgStreamList = msgStreams.get(TOPIC);
//      理论上应该是多线程处理，不想写多线程，先写一个线程吧。
//        for(int i =0; i <msgStreamList.size();i++)
//        {
//
//        }
        KafkaStream<byte[],byte[]> kstream = msgStreamList.get(0);//因为只有已给线程，所以，就取第一个

        ConsumerIterator<byte[],byte[]> iterator = kstream.iterator();
        int linescount =0;
        int Filecount = 1;
        File outfile = new File("./out/output"+Filecount);
        BufferedWriter bw = new BufferedWriter(new FileWriter(outfile));
        StringBuilder strbui =new StringBuilder();
        while(iterator.hasNext())
        {

            if(linescount % 10000000 == 0) //每1000万行，写一个文件
            {
                bw.write(strbui.toString());
                bw.flush();
                bw.close();
                Filecount++;
                System.out.println("File " + Filecount + " has been save in path //out//output ");
                outfile = new File("./out/output"+Filecount);
                bw = new BufferedWriter(new FileWriter(outfile));
                strbui =new StringBuilder();
            }
            String message= new String(iterator.next().message());
            strbui.append(message);
            strbui.append("|||");
            linescount++;
        }
        //剩余部分，写入文件
        bw.write(strbui.toString());
        bw.flush();
        bw.close();

        consumer.commitOffsets();

        return Filecount;
    }

    public static void main(String[] args)
    {
        KafkaConsumer consumer = new KafkaConsumer();
        int i= 0;
        try {
            i= consumer.consume();
        }
        catch (Exception ex)
        {
            System.out.println("Exception: " + ex.getMessage());
        }

        System.out.printf("%d files has been saved.===========Programme Done!==========\n",i);

    }


}

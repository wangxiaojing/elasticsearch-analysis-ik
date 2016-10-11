package org.wltea.analyzer.dic;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by wangxiaojing on 2016/10/11.
 */
public class RedisDict implements Runnable {
    public static ESLogger logger = Loggers.getLogger("ik-analyzer");
    private FileOutputStream stdOut;
    private static final String fileDir = "custom";
    private static final String STD_FILE = "mydict.txt";
    private static int suffix = 0;
    private final static String CHARSET = "UTF-8";
    private final static int MAX_SIZE = 1024000000;
    //private static RedisDict redisDict=null;
    private JedisCluster jc = null;

    /*
    public static RedisDict getInstance(){
        RedisDict result = null;
        synchronized(RedisUpdateDict.class){
            if(null==redisDict){
                redisDict = new RedisDict();
                result = redisDict;
            }
        }
        return result;
    }
    */

    private void init() {
        if (null == jc) {
            List<String> redisHosts = Dictionary.getSingleton().getRedisHosts();
            logger.info("host is :" + redisHosts.toString());
            Set<HostAndPort> hps = new HashSet<HostAndPort>();
            for (String redisHost : redisHosts) {
                String[] hp = redisHost.split(":");
                logger.info(hp[0] + ", port : " + hp[1]);
                hps.add(new HostAndPort(hp[0], Integer.valueOf(hp[1]).intValue()));
            }
            GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
            poolConfig.setJmxEnabled(false);
            jc = new JedisCluster(hps, 5000, 10, poolConfig);
            logger.info("connect redis success ");
        }
        if (null == stdOut) {
            try {
                File file = new File(fileDir, STD_FILE);
                Long size = file.getTotalSpace();
                if (size >= MAX_SIZE) {
                    File dir = new File(fileDir);
                    if (suffix == 0) {
                        for (File fileName : dir.listFiles()) {
                            if (fileName.getName().startsWith(STD_FILE)) {
                                suffix++;
                            }
                        }
                    } else {
                        suffix++;
                    }
                    file.renameTo(new File(fileDir, STD_FILE + "." + suffix));
                    String ext_dict = Dictionary.getSingleton().getProperty("ext_dict");
                    String extDictNew = ext_dict + ";" + fileDir + File.separator + STD_FILE + "." + suffix;
                    Dictionary.getSingleton().setProperty("ext_dict", extDictNew);
                    if (stdOut != null) {
                        stdOut.close();
                    }
                }

            } catch (Exception e) {
                File file = new File(fileDir, STD_FILE);
               try{
                   file.createNewFile();
                   stdOut = new FileOutputStream(file);
               }catch (Exception e1){
                   e1.printStackTrace();
               }
                logger.info("file un Exit");
            }
        }
        logger.info("exit init ");
    }

    public void run() {
        Dictionary.logger.info("[Redis Thread]" + "*****cycle of redis");
        init();
        logger.info("pull start");
        pull();
        sleep();
    }

    public void pull() {
        if (null == jc)
            return;
        // 从集合里拉取
        Dictionary.logger.info("pull in start");
        ArrayList<String> words = new ArrayList<String>();
        Set set = null;


        Dictionary.logger.info("get redis ik_main :");
        // 1拉取主词
        set = jc.smembers("ik_main");
        Dictionary.logger.info("get ik_main :" + set.toString());
        Iterator t = set.iterator();
        while (t.hasNext()) {
            Object obj = t.next();
            words.add(obj.toString());
            try {
                Dictionary.logger.info("add ik main dict : " + obj.toString());
                stdOut.write(obj.toString().getBytes(CHARSET), 0, obj.toString().length());
            } catch (Exception e) {
                Dictionary.logger.info("can not add ik dict : " + e.toString());
            }
        }
        Dictionary.getSingleton().addMainWords(words);
        words.clear();

        // 2拉取停词
        set = jc.smembers("ik_stop");
        t = set.iterator();
        while (t.hasNext()) {
            Object obj = t.next();
            words.add(obj.toString());
        }
        Dictionary.getSingleton().addStopWords(words);
        words.clear();
    }

    private void sleep() {
        // sleep 5 seconds,then loop to next
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void exit() throws IOException {
        if (stdOut != null) {
            stdOut.close();
        }
    }
}

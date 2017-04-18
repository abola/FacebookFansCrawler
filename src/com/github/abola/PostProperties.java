package com.github.abola;

import com.github.abola.crawler.CrawlerPack;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.List;
import java.util.Map;

/**
 * Created by i5-4670 on 2017/4/17.
 */
public class PostProperties {


    public static void main(String[] args) throws Exception{

        MySQLDataSource mysql = new MySQLDataSource();
        mysql.execute("SET NAMES utf8mb4");

        Map<String,Object> auth = mysql.query("select * from auth").get(0);
        String api_key = auth.get("app_id")+"%7C"+auth.get("app_key");
        List<Map<String,Object>> fansPages = mysql.query("select * from fans_page where not EXISTS (select fans_id from posts where fans_id = id ) ");



        for(Map<String, Object> fansPage: fansPages){

            String id  = fansPage.get("id").toString();
            String uri = "https://graph.facebook.com/v2.8/"+id+"/posts?fields=id,message,created_time&limit=100&access_token="+ api_key;


            Document jsoup = CrawlerPack.start().getFromJson(uri);

            StringBuilder insertSQL = new StringBuilder();


            for(Element elem : jsoup.select("data")){
                String fans_id = elem.select("id").text().split("_")[0];
                String object_id = elem.select("id").text().split("_")[1];
                String message = elem.select("message").text().replaceAll("'"," ");
                String created_time = elem.select("created_time").text().substring(0,19).replace("T"," ");


                insertSQL.append("("+fans_id+","+object_id+",'"+message+"','"+created_time+"'+INTERVAL 8 HOUR),");
            }

            if(insertSQL.length()>0) {
                //System.out.println(insertSQL.substring(0, insertSQL.length() - 1));
                String appendSql = insertSQL.substring(0, insertSQL.length() - 1);
                mysql.execute("insert into posts(fans_id,object_id,message,created_time) values " + appendSql + " on duplicate key update message=VALUES(message), created_time=VALUES(created_time)");
            }
        }
    }
}

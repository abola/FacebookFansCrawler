package com.gihub.abola;

import com.github.abola.crawler.CrawlerPack;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.List;
import java.util.Map;

/**
 * Created by i5-4670 on 2017/4/17.
 */
public class FeedProperties {
    static Logger log = Logger.getLogger( FeedProperties.class );

    public static void main(String[] args) throws Exception{

        MySQLDataSource mysql = new MySQLDataSource();
        mysql.execute("SET NAMES utf8mb4");

        Map<String,Object> auth = mysql.query("select * from auth").get(0);
        String api_key = auth.get("app_id")+"%7C"+auth.get("app_key");
        List<Map<String,Object>> fansPages = mysql.query("select * from fans_page");



        for(Map<String, Object> fansPage: fansPages){

            String id  = fansPage.get("id").toString();
            String uri = "https://graph.facebook.com/v2.8/"+id+"/posts?access_token="+ api_key;


            Document jsoup = CrawlerPack.start().getFromJson(uri);

            StringBuilder insertSQL = new StringBuilder();


            for(Element elem : jsoup.select("data ")){
                String fans_id = elem.select("id").text();
                String message = elem.select("message").text().replaceAll("'"," ");
                String created_time = elem.select("created_time").text().substring(0,19).replace("T"," ");


                insertSQL.append("('"+fans_id+"','"+message+"','"+created_time+"'+INTERVAL 8 HOUR),");
            }

            if(insertSQL.length()>0) {
                //System.out.println(insertSQL.substring(0, insertSQL.length() - 1));
                String appendSql = insertSQL.substring(0, insertSQL.length() - 1);
                mysql.execute("replace into posts(id,message,created_time) values " + appendSql);
            }
        }
    }
}

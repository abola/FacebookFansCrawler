package main.java.com.github.abola;

import com.github.abola.crawler.CrawlerPack;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.List;
import java.util.Map;

/**
 * Created by i5-4670 on 2017/4/17.
 */
public class PostLikes {


    public static void main(String[] args) throws Exception{

        MySQLDataSource mysql = new MySQLDataSource();
        mysql.execute("SET NAMES utf8mb4");

        Map<String,Object> auth = mysql.query("select * from auth").get(0);
        String api_key = auth.get("app_id")+"%7C"+auth.get("app_key");
        List<Map<String,Object>> fansPagePosts = mysql.query("select * from posts where done_time is null limit 1");



        for(Map<String, Object> post: fansPagePosts){

            String fans_id = post.get("fans_id").toString();
            String object_id = post.get("object_id").toString();
            String id  = fans_id + "_" + object_id;
            String uri = "https://graph.facebook.com/v2.8/"+id+"/likes?fields=id&limit=1000&access_token="+ api_key;


            Document jsoup = CrawlerPack.start().getFromJson(uri);

            StringBuilder insertSQL = new StringBuilder();


            for(Element elem : jsoup.select("data id")){
                String user_id = elem.text();
                insertSQL.append("("+fans_id+","+object_id+","+user_id+"),");
            }

            if(insertSQL.length()>0) {
                //System.out.println(insertSQL.substring(0, insertSQL.length() - 1));
                String appendSql = insertSQL.substring(0, insertSQL.length() - 1);
                mysql.execute("replace into post_likes(fans_id,object_id,user_id) values " + appendSql);
            }

            while( jsoup.select("paging > next").size() > 0 ){
                // RESET SQL
                insertSQL = new StringBuilder();
                uri = jsoup.select("paging > next").text();
                jsoup = CrawlerPack.start().getFromJson(uri);
                for(Element elem : jsoup.select("data id")){
                    String user_id = elem.text();
                    insertSQL.append("("+fans_id+","+object_id+","+user_id+"),");
                }
                if(insertSQL.length()>0) {
                    //System.out.println(insertSQL.substring(0, insertSQL.length() - 1));
                    String appendSql = insertSQL.substring(0, insertSQL.length() - 1);
                    mysql.execute("replace into post_likes(fans_id,object_id,user_id) values " + appendSql);
                }
            }

            // done
            mysql.execute("update posts set done_time = now() where fans_id = "+ fans_id +" and object_id = " + object_id );
        }
    }
}

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public class YandexCupQuestionD {
    private static HashMap<String,Integer> cacheIdFinder = new HashMap<>();
    private static HashMap<Integer,String> cacheEndpointFinder = new HashMap<>();
    private static PriorityQueue<Pair> cacheWithMinLoadFinder = new PriorityQueue<>(new Comparator<Pair>() {
        @Override
        public int compare(Pair o1, Pair o2) {
            return Integer.compare(o1.cacheLoad,o2.cacheLoad);
        }
    });
    private static Integer numOfCacheServersFinder(HttpClient client) throws IOException, InterruptedException {
        String cacheURL = "http://localhost:80/cache/";
        Integer numOfCacheServers = 0;
        for(int i=1;i<=10;i++){
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(cacheURL+i+"/"+"parth"))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            String responseString = response.body().trim();
            if(response.statusCode()==404 && !responseString.equalsIgnoreCase("Error")){
                numOfCacheServers+=1;
            }
        }
        return numOfCacheServers;
    }
    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String configFilePath = args[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Config config = objectMapper.readValue(new File(configFilePath), Config.class);
        String inputKey;
        String testdbURL = "http://localhost:80/db/";
        String dbURL = config.getDbUrl();
        String testcacheURL = "http://localhost:80/cache/";
        HttpClient client = HttpClient.newHttpClient();
        Integer numOfCacheServers = config.getCacheUrls().size();
        List<String> cacheServersUrls = config.getCacheUrls();
        for(int i=1;i<=numOfCacheServers;i++){
            cacheWithMinLoadFinder.add(new Pair(i,0));
            cacheEndpointFinder.put(i,cacheServersUrls.get(i-1));
        }
        while((inputKey = reader.readLine())!=null){
            if(cacheIdFinder.containsKey(inputKey)){
                Integer cacheServerID = cacheIdFinder.get(inputKey);
                HttpRequest testrequest = HttpRequest.newBuilder()
                        .uri(URI.create(testcacheURL+cacheServerID+"/"+inputKey))
                        .GET()
                        .build();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(cacheEndpointFinder.get(cacheServerID)+inputKey))
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if(response.statusCode()==502){
                    HttpRequest requestWhenCacheUnavailable = HttpRequest.newBuilder()
                            .uri(URI.create(dbURL+inputKey))
                            .GET()
                            .build();
                    HttpResponse<String> responseWhenCacheUnavailable = client.send(requestWhenCacheUnavailable, HttpResponse.BodyHandlers.ofString());
                    if(responseWhenCacheUnavailable.statusCode()==200){
                        System.out.println(responseWhenCacheUnavailable.body());
                    }
                }
                if(response.statusCode()==200){
                    System.out.println(response.body());
                }
            }else{
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(dbURL+inputKey))
                        .GET()
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if(response.statusCode()==404){
                    continue;
                }
                Integer currMinLoadCache = 1;
                Integer currMinLoad = 0;
                if(cacheWithMinLoadFinder.size()>0){
                    Pair topData = cacheWithMinLoadFinder.peek();
                    currMinLoadCache = topData.cacheID;
                    currMinLoad = topData.cacheLoad;
                }
                HttpRequest testrequestPUT = HttpRequest.newBuilder()
                        .uri(URI.create(testcacheURL+currMinLoadCache+"/"+inputKey))
                        .PUT(HttpRequest.BodyPublishers.ofString(response.body()))
                        .build();
                HttpRequest requestPUT = HttpRequest.newBuilder()
                        .uri(URI.create(cacheEndpointFinder.get(currMinLoadCache)+inputKey))
                        .PUT(HttpRequest.BodyPublishers.ofString(response.body()))
                        .build();
                HttpResponse<String> responsePUT = client.send(requestPUT, HttpResponse.BodyHandlers.ofString());
                if(responsePUT.statusCode()!=502){
                    JsonNode jsonNode = objectMapper.readTree(response.body());
                    cacheIdFinder.put(jsonNode.get("id").asText(),currMinLoadCache);
                    cacheWithMinLoadFinder.poll();
                    cacheWithMinLoadFinder.add(new Pair(currMinLoadCache,currMinLoad+1));
                }
                if(response.statusCode()==200){
                    System.out.println(response.body());
                }
            }
        }
    }
}
class Pair{
    Integer cacheID;
    Integer cacheLoad;
    public Pair(Integer id,Integer load){
        this.cacheID = id;
        this.cacheLoad = load;
    }
}
class Config {
    @JsonProperty("db_url")
    private String dbUrl;
    @JsonProperty("cache_urls")
    private List<String> cacheUrls;
    public String getDbUrl() {
        return dbUrl;
    }
    public void setDbUrl(String db_url) {
        this.dbUrl = db_url;
    }
    public List<String> getCacheUrls() {
        return cacheUrls;
    }
    public void setCacheUrls(List<String> cache_urls) {
        this.cacheUrls = cache_urls;
    }
}

/*
Alex Soh 34706296
Bryan Diep 27192773
*/
//Loads data from existing files and use them to respond to queries

import java.util.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.JsonObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonWriter;
import java.io.*;

public class CLQuery{
   private static HashMap<Integer, String> docid2url = new HashMap<Integer, String>();
   private static HashMap<String, Integer> term2termid = new HashMap<String, Integer>();
   private static Map<Integer, Map<Integer, Double>> termid2tfidf = 
      new HashMap<Integer, Map<Integer, Double>>();

   private  static String docid2urlPath = "/home/brydi/cs121/Data/output/docid2url.json";
   private  static String term2termidPath = "/home/brydi/cs121/Data/output/term2termid.json";
   private  static String termid2tfidfPath = "/home/brydi/cs121/Data/output/termid2tfidf.json";
   
   public static void load_docid2url(String path){
      Gson gson = new Gson();
      try{
         BufferedReader br = new BufferedReader(new FileReader(path));
         JsonObject obj = gson.fromJson(br, JsonObject.class);
         for(Map.Entry<String, JsonElement> entry : obj.entrySet()){
            int docid = Integer.parseInt(entry.getKey());
            String url = obj.getAsJsonPrimitive(entry.getKey()).getAsString();
            docid2url.put(docid, url);
         }
         br.close();
         
      }catch(IOException e){
         e.printStackTrace();
      }
   }
   
   public static void load_term2termid(String path){
      Gson gson = new Gson();
      try{
         BufferedReader br = new BufferedReader(new FileReader(path));
         JsonObject obj = gson.fromJson(br, JsonObject.class);
         for(Map.Entry<String, JsonElement> entry : obj.entrySet()){
            String term = entry.getKey();
            int termid = obj.getAsJsonPrimitive(entry.getKey()).getAsInt();
            term2termid.put(term, termid);
         }
         br.close();
         
      }catch(IOException e){
         e.printStackTrace();
      }
   }
   
   public static void load_termid2tfidf(String path){
      Gson gson = new Gson();
      try{
         BufferedReader br = new BufferedReader(new FileReader(path));
         JsonObject obj = gson.fromJson(br, JsonObject.class);
         for(Map.Entry<String, JsonElement> entry : obj.entrySet()){
            int termid = Integer.parseInt(entry.getKey());
            termid2tfidf.put(termid, new HashMap<Integer, Double>());
            JsonObject element = obj.getAsJsonObject(entry.getKey());
            for(Map.Entry<String, JsonElement> docid_tfidf_pair : element.entrySet()){
               int docid = Integer.parseInt(docid_tfidf_pair.getKey());
               Double tfidf = element.getAsJsonPrimitive(docid_tfidf_pair.getKey()).getAsDouble();
               termid2tfidf.get(termid).put(docid, tfidf);
            }
         }
         br.close();
         
      }catch(IOException e){
         e.printStackTrace();
      }

   }
   
   //used to normalize terms in query
   public static ArrayList<String> tokenizer(String text)
   {
      ArrayList<String> result = new ArrayList<String>(Arrays.asList(text.toLowerCase().split("\\W+")));
      return result;
   }
   
   
   public static Double weightInQuery(String term, List<String> query, int df){
   //Gets the tfidf score for a query term, uses log normalization
      int docCount = docid2url.size();
      int qFrequency = 0;
      for(String t : query){
         if(term == t)
            qFrequency++;
      }
      return (1.0 + Math.log(qFrequency)) * Math.log((double)docCount/df);
   }
   
   public static Map<Integer, Double> normalize(Map<Integer,Double> score, double querymag){
      Map<Integer, Double> final_scores = new HashMap<Integer, Double>();
      Map<Integer, Double> docid2magnitude = new HashMap<Integer, Double>();
      for (Map.Entry<Integer,Map<Integer,Double>> entry : termid2tfidf.entrySet()){ //Goes thru term -> <docid -> tfidif> map
         int termid = entry.getKey();
         for(Map.Entry<Integer, Double> e : entry.getValue().entrySet()){ //Goes through the docid,tfidf map
            int docid = e.getKey();
            double tfidf = e.getValue();
            if(score.containsKey(docid)){ //If score map has the docid in it
               if(docid2magnitude.containsKey(docid)){
                  double old_value = docid2magnitude.get(docid); //Gets the old value from the magnitude map
                  docid2magnitude.put(docid, old_value + (tfidf * tfidf)); //Takes old value and adds the square of the tfidf
               }
               else
                  docid2magnitude.put(docid, tfidf * tfidf); //First time seeing doc
            }
         }
      }
      
      for(Map.Entry<Integer, Double> entry : docid2magnitude.entrySet()){
         int docid = entry.getKey();
         double magnitudesq = entry.getValue();
         final_scores.put(docid, score.get(docid) / Math.sqrt(magnitudesq) / querymag); //Calculates the cosine similarity
      }
      
      return final_scores;
      

   }
   
   

   public static Map<Integer, Double> getQueryScores(String query){
      Map<Integer, Double> cumulative_results = new HashMap<Integer, Double>();
      List<String> normalized_terms = tokenizer(query);
      double querymag = 0.0;
      
      for(String term : normalized_terms){ //Goes through all the terms in the query
         if(term2termid.keySet().contains(term)){
            int termid = term2termid.get(term); //Gets the termid for the term
            Map<Integer, Double> doc_tfidf = termid2tfidf.get(termid); //Gets the docid and tfidf score map
            int df = termid2tfidf.get(termid).size(); //Gets the document frequency, df is the size of the map with given termid
            Double alpha = weightInQuery(term, normalized_terms, df); //Gets the tfidf score for that term in the query
            querymag += alpha * alpha; //part of the euclidean distance for that term in query
            
            for(Map.Entry<Integer, Double> entry : doc_tfidf.entrySet()){ //Goes through the docidf and tfidf map for that term in query
               int docid = entry.getKey(); //Gets a docid!
               Double tfidf = entry.getValue();//Gets tfidf score
               
               if(cumulative_results.containsKey(docid)){
                  Double old_value = cumulative_results.get(docid); //Gets the old sum of tfidf scores for query
                  Double new_score = alpha * tfidf; //Multiplies tfidf score for query by tfidf for doc
                  cumulative_results.put(docid, old_value + new_score); //Adds to the accumulator
               }
               else{
                  cumulative_results.put(docid, alpha * tfidf); //First time seeing
               }
            }
         }
      }
      querymag = Math.sqrt(querymag); //Square roots the euclidean distance for the query
      return normalize(cumulative_results, querymag);//Calls the normalize function to get the map with the docids and cosine similairty score
   }
   
   public static TreeMap<Double, Integer> tree(Map<Integer, Double> score_map){
      TreeMap<Double, Integer> treemap = new TreeMap<Double, Integer>();
      for(Map.Entry<Integer, Double> entry : score_map.entrySet()){ //Goes through the map with the scores
         Double score = entry.getValue(); //Gets the cosine similarity score from the map
         int docid = entry.getKey();      //Gets the document id from the map
         treemap.put(score, docid);       //Places it into the treemap 
      }
      return treemap;
   }
   
   public static void main(String[] args){
      System.out.println("Loading data");
      load_docid2url(docid2urlPath);
      load_term2termid(term2termidPath);
      load_termid2tfidf(termid2tfidfPath);
      //System.out.println("done");
      Console console = System.console();

      while(true){
         String input = console.readLine("Enter query, '!' to quit: ");
         //System.out.println(input);
         if(input.equals("!")){
            break;
         }
         Map<Integer, Double> scores = getQueryScores(input);
         TreeMap<Double, Integer> info = tree(scores);
         System.out.println("Results for " + input);
         for(int i = 0; i < 5; i++){
            if(info.size() > 0){
               double topscore = info.lastKey();
               //System.out.println(topscore);
               int topid = info.remove(topscore);
               System.out.println(docid2url.get(topid)); 
            }
         }

        
      }
      
   }
}
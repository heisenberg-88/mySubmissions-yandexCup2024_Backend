import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

public class YandexCupQuestionA {
    private static BigDecimal currTotal = new BigDecimal(0);
    private static Integer numOfPeoplePresent = 0;
    private static HashMap<Integer,BigDecimal> idAndTemps = new HashMap<Integer, BigDecimal>();
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String currLine;
        while((currLine = reader.readLine())!=null){
            if(currLine.equalsIgnoreCase("!")){
                break;
            }
            processData(currLine);
        }
    }
    private static void processData(String s){
        String[] currData = s.split("\\s+");
        if(s.length()>1){
            if(currData[0].equalsIgnoreCase("+")){
                BigDecimal currItemTemp = new BigDecimal(currData[2]);
                idAndTemps.put(Integer.parseInt(currData[1]),currItemTemp);
                currTotal = currTotal.add(currItemTemp);
                numOfPeoplePresent+=1;
            }
            if(currData[0].equalsIgnoreCase("-")){
                BigDecimal currItemTemp = idAndTemps.get(Integer.parseInt(currData[1]));
                idAndTemps.remove(Integer.parseInt(currData[1]));
                currTotal = currTotal.subtract(currItemTemp);
                numOfPeoplePresent-=1;
            }
            if(currData[0].equalsIgnoreCase("~")){
                BigDecimal updateTemp = new BigDecimal(currData[2]);
                BigDecimal oldTemp = idAndTemps.get(Integer.parseInt(currData[1]));
                idAndTemps.put(Integer.parseInt(currData[1]),updateTemp);
                currTotal = currTotal.subtract(oldTemp);
                currTotal = currTotal.add(updateTemp);
            }
        }else if(s.length()==1 && currData[0].equalsIgnoreCase("?")){
            BigDecimal finalAns = currTotal.divide(BigDecimal.valueOf(numOfPeoplePresent), 9, RoundingMode.HALF_UP);
            System.out.println(finalAns);
        }
    }
}

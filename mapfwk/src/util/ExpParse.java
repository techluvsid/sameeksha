/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Administrator
 */
public class ExpParse {

    private static final String pattern = "[| =,{(+*&/;-]{1}[a-zA-Z_]+[a-zA-Z_0-9.]+[| =,})+/*;&-]{1}";
    private static final Pattern r = Pattern.compile(pattern);

    public static List<String> getColumns(String exp) {
        List<String> ret = new ArrayList<String>();
        Matcher m = r.matcher(exp);
        while (m.find()) {
            //System.out.println("Match: " + m.toString());
            String ma=m.group().replaceAll("[=,{(|+*/ ;)}-]", "");
            if(!ret.contains(ma) && !ma.toUpperCase().matches("AND|OR|NOT|TRUE|FALSE|NULL") && !ma.endsWith(".")){
                ret.add(ma);
            }
        }
        return ret;
    }

    public static void main(String[] args) {
        // TODO code application logic here
        // String to be scanned to find the pattern.
        String line = "IIF(NOt (TO_INTEGER(INDEXOF(RTRIM(FULL_NAME1),'JR','SR','DR','II','III','IV','MD',1)) > 0 AND 0>1),SUBSTR(RTRIM(FULL_NAME1),0,INSTR(RTRIM(FULL_NAME3),' ',0,1)),IIF(INSTR(RTRIM(FULL_NAME4),' ',0,2) > 0,SUBSTR(RTRIM(FULL_NAME5),0,INSTR(RTRIM(FULL_NAME6),' ',0,2)),SUBSTR(RTRIM(FULL_NAME7),0,INSTR(RTRIM(FULL_NAME8),' ',0,1))))";
        line = " COUNTRY_ID_1||COUNTRY_NAME_1 ";
        //String pattern = "[=,{(|+*/ ;-]+([a-zA-Z_]+[a-zA-Z_0-9.]+)[=,}) +/*;|&-]+";
        for(String col:ExpParse.getColumns(line)){
            System.out.println("Match: " + col);
        }
        

    }
}

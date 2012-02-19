/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkRetrieveContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import java.util.Vector;
import util.MapHelper;

/**
 *
 * @author Administrator
 */
public class local {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        Repository rep = new Repository();
		rep.retrieveRepo(new MapFwkRetrieveContext(MapFwkRetrieveContext.INPUT_FORMAT_XML,
					MapFwkRetrieveContext.INPUT_FILE,
					"C:\\work\\m_test.XML"));
                
        Vector folders = rep.getFolder(new NameFilter() {

            public boolean accept(String name) {
                return name.equals("Dev");
            }
        });

        
        //folder count - in this case it is always 1
        int folderSize = folders.size();

        long begin = System.currentTimeMillis();
        for (int i = 0; i < folderSize; i++) {
            Vector listOfMappings = ((Folder) folders.get(i)).getMappings(new NameFilter() {
                public boolean accept(String name) {
                    return name.contains("m_test");
                }
            }); //get the list of mappings
           
            Mapping m = (Mapping) listOfMappings.get(0);
            TransformHelper helper = new TransformHelper(m);
            MapHelper mh=new MapHelper(m);
            mh.getChild("COUNTRIES1.REGION_ID");
    }
}
}

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionObject;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkRetrieveContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.repository.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryConnectionManager;
import java.util.List;

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
                MapFwkRetrieveContext.INPUT_FORMAT_XML,
                "D:\\temp\\M_CUSTOMERS_MASKING.xml"));


        List<Folder> folders = rep.getAddedFolders();

        //gets folder count from repository
        int folderSize = folders.size();
        System.out.println(folderSize);
        System.out.println("List of folder present in repository: " + rep.getRepoConnectionInfo().getTargetRepoName());
        //folder count - in this case it is always 1


        for (int i = 0; i < folderSize; i++) {
            List<Source> listOfSources = ((Folder) folders.get(i)).getSources(); //ge tthe list of sources
            int listSize = listOfSources.size();
            System.out.println(" ***** List of Sources ******");
            for (int j = 0; j < listSize; j++) {
                System.out.println((listOfSources.get(j)).getName());
            }
        }

        for (int i = 0; i < folderSize; i++) {
            List<Transformation> listOfTransformations = ((Folder) folders.get(i)).getTransformations();
            int listSize = listOfTransformations.size();
            System.out.println(" ***** List of Reusable Transformations ******");
            for (int j = 0; j < listSize; j++) {
                System.out.println((listOfTransformations.get(j)).getName());
            }
        }


        for (int i = 0; i < folderSize; i++) {
            List<Target> listOfTargets = ((Folder) folders.get(i)).getTargets(); //get the list of targets
            int listSize = listOfTargets.size();
            System.out.println(" ***** List of Targets ******");
            for (int j = 0; j < listSize; j++) {
                System.out.println((listOfTargets.get(j)).getName());
            }
        }

        for (int i = 0; i < folderSize; i++) {
            List<Mapplet> listOfMapplets = ((Folder) folders.get(i)).getMapplets(); //get the list of mapplets
            int listSize = listOfMapplets.size();
            System.out.println(" ***** List of Mapplets ******");
            for (int j = 0; j < listSize; j++) {
                System.out.println((listOfMapplets.get(j)).getName());
            }
        }

        for (int i = 0; i < folderSize; i++) {
            List<Mapping> listOfMappings = ((Folder) folders.get(i)).getMappings(); //get the list of mappings
            int listSize = listOfMappings.size();
            System.out.println(" ***** List of Mappings ******");
            for (int j = 0; j < listSize; j++) {
                System.out.println((listOfMappings.get(j)).getName());
                List<Transformation> listOfTransformations = (listOfMappings.get(j)).getTransformations();

                System.out.println(" >>>***** List of Transformations ******");
                for (int k = 0; k < listOfTransformations.size(); k++) {
                    System.out.println("   "+(listOfTransformations.get(k)).getName());
                    
                }
            }
        }

    }
}

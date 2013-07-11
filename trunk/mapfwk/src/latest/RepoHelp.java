/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package latest;

import com.informatica.powercenter.sdk.mapfwk.core.MapFwkRetrieveContext;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.UnsupportedSourceException;
import com.informatica.powercenter.sdk.mapfwk.exception.UnsupportedTargetException;
import com.informatica.powercenter.sdk.mapfwk.exception.UnsupportedTransformationException;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;

/**
 *
 * @author Rishav
 */
public class RepoHelp {
    private Repository rep;
    public RepoHelp(String file) throws MapFwkReaderException, UnsupportedTransformationException, UnsupportedSourceException, UnsupportedTargetException{
        rep = new Repository();
        rep.retrieveRepo(new MapFwkRetrieveContext(MapFwkRetrieveContext.INPUT_FORMAT_XML,
                MapFwkRetrieveContext.INPUT_FORMAT_XML,
                file));
    }
}

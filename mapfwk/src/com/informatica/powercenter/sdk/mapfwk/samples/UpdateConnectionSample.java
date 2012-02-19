/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.File;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.core.CodePageConstants;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionAttributes;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionObject;
import com.informatica.powercenter.sdk.mapfwk.core.RepoProperties;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionAlreadyExistsException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionCreationFailedException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionDoesNotExistException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionFailedException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionUpdateFailedException;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.PMREP;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;


/**
 * @author sramamoo
 *
 */
public class UpdateConnectionSample
{
	public static void main(String[] args)
	{
		CachedRepositoryConnectionManager rpMgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
		Repository myRepo = new Repository();
		RepoProperties repoProp = new RepoProperties();

		repoProp.setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, "C:\\Informatica\\PowerCenter8.1.1\\client\\bin");
		repoProp.setProperty(RepoPropsConstant.TARGET_REPO_NAME,"PowerCenter");
		repoProp.setProperty(RepoPropsConstant.REPO_SERVER_HOST,"IN164249");
		repoProp.setProperty(RepoPropsConstant.REPO_SERVER_PORT,"6001");
		repoProp.setProperty(RepoPropsConstant.ADMIN_USERNAME,"Administrator");
		repoProp.setProperty(RepoPropsConstant.ADMIN_PASSWORD,"Administrator");
		
		myRepo.setProperties(repoProp);
		myRepo.setRepositoryConnectionManager(rpMgr);
		// No need to do this, as the above call sets myRepo as the repository for the rpMgr.
		//rpMgr.setRepository(myRepo);

		// CONN_TYPE is set when a call is made to ConnectionAttributes.getDefaultRelationalProperties() or 
		// ConnectionAttributes.getDefaultApplicationProperties()
		Properties prop = ConnectionAttributes.getDefaultRelationalProperties(ConnectionAttributes.DB_CONN_TYPE_ORACLE);
		prop.list(System.out);
		
		prop.setProperty(ConnectionAttributes.CONN_ATTR_CONNECTION_NAME, "myConn");
		prop.setProperty(ConnectionAttributes.CONN_ATTR_USER_NAME, "sramamoo");
		prop.setProperty(ConnectionAttributes.CONN_ATTR_CONNECT_ENV_SQL,"");
		prop.setProperty(ConnectionAttributes.CONN_ATTR_CODE_PAGE,"");
		prop.setProperty(ConnectionAttributes.CONN_ATTR_CONNECT_STRING,"");
		ConnectionObject connObj = new ConnectionObject("myConn",ConnectionAttributes.CONN_TYPE_RELATIONAL);
		connObj.setConnectionObjectAttr(prop);
		try
		{
			myRepo.updateConnection(connObj);
		} catch (ConnectionUpdateFailedException e)
		{
			e.printStackTrace();
		} catch (ConnectionFailedException e)
		{
			e.printStackTrace();
		}
	}
}

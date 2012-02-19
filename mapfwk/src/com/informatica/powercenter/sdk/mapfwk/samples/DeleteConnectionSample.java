/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionAttributes;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionObject;
import com.informatica.powercenter.sdk.mapfwk.core.RepoProperties;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionAlreadyExistsException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionCreationFailedException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionDeletionFailedException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionDoesNotExistException;
import com.informatica.powercenter.sdk.mapfwk.exceptions.ConnectionFailedException;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * @author sramamoo
 *
 */
public class DeleteConnectionSample
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
		rpMgr.setRepository(myRepo);
						
		try
		{
			myRepo.deleteConnection(ConnectionAttributes.CONN_TYPE_RELATIONAL, "myConn1");
			
		} catch (ConnectionFailedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConnectionDoesNotExistException e)
		{
			System.err.println("Connection doesnot exist");
			e.printStackTrace();
		} catch (ConnectionDeletionFailedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

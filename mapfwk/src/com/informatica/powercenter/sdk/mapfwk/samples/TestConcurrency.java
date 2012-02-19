/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * @author sramamoo
 *
 */
public class TestConcurrency
{
	static int counter = 0;
	public void init(Repository rep) throws IOException
	{
        Properties properties = new Properties();
//        String filename = "C:\\OUTPUT\\eBiz\\javamappingsdk_Output\\pcconfig.properties";
//        InputStream propStream = new FileInputStream(filename);
        String filename = "pcconfig.properties";
   		InputStream propStream = getClass().getClassLoader().getResourceAsStream( filename);

        if ( propStream != null ) {
        	properties.load( propStream );

            rep.getProperties().setProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH, properties.getProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH));
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_FOLDER_NAME, properties.getProperty(RepoPropsConstant.TARGET_FOLDER_NAME));
            rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME, properties.getProperty(RepoPropsConstant.TARGET_REPO_NAME));
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_HOST, properties.getProperty(RepoPropsConstant.REPO_SERVER_HOST));
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD, properties.getProperty(RepoPropsConstant.ADMIN_PASSWORD));
            rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME, properties.getProperty(RepoPropsConstant.ADMIN_USERNAME));
            rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_PORT, properties.getProperty(RepoPropsConstant.REPO_SERVER_PORT));
            rep.getProperties().setProperty(RepoPropsConstant.SERVER_PORT, properties.getProperty(RepoPropsConstant.SERVER_PORT));
            rep.getProperties().setProperty(RepoPropsConstant.DATABASETYPE, properties.getProperty(RepoPropsConstant.DATABASETYPE));
            if(properties.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER) != null)
            	rep.getProperties().setProperty(RepoPropsConstant.PMREP_CACHE_FOLDER, properties.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER));
        }
        else {
            throw new IOException( "pcconfig.properties file not found.");
        }
	}
	public static void main(String[] args) throws IOException
	{		
		TestConcurrency test = new TestConcurrency();
		Repository rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );   
		RepositoryConnectionManager repMgr = new CachedRepositoryConnectionManager(new PmrepRepositoryConnectionManager());
		
		test.init(rep);
        
		Thread[] threads = new Thread[5];
		for(int i=0;i<5;i++)
		{
			threads[i] = new Thread(new function(i,repMgr, rep));
			threads[i].start();
		}		
	}
}
class function implements Runnable
{
	int id;
	RepositoryConnectionManager repMgr;
	Repository rep;
	public function(int id, RepositoryConnectionManager repMgr, Repository rep)
	{
		this.id = id;
		this.repMgr = repMgr;
		this.rep = rep;
	}
	public void run()
	{
		ModifyObjectsSample sample = null;
		try
		{
			sample = new ModifyObjectsSample();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		sample.setMapFileName("ModifyObjects_"+id);
		sample.setRepository(rep);
		sample.setRepositoryConnectionManager(repMgr);
		try
		{
			sample.execute();
		} catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		
}
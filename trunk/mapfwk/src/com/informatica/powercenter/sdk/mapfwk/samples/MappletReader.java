package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;
import com.informatica.powercenter.sdk.mapfwk.core.NameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.reputils.CachedRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.reputils.RepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.util.pmrepwrap.PmrepRepositoryConnectionManager;

/**
 * This class if for reading a mapplet from repository
 * @author rjain
 *
 */
public class MappletReader {
	protected Repository rep;

	protected String mapFileName;
	
	private Mapplet m_mapplet;
	
	protected Folder folder;
	protected int runMode = 0;
	
	RepositoryConnectionManager repmgr;

	public MappletReader() throws IOException {
		init();

		rep.setRepositoryConnectionManager(repmgr);
		setMapFileName("MappletReader");
	}

	protected void init() throws IOException {
		createRepository();
		Properties properties = new Properties();
		String filename = "pcconfig.properties";
		InputStream propStream = getClass().getClassLoader()
				.getResourceAsStream(filename);

		if (propStream != null) {
			properties.load(propStream);

			rep
					.getProperties()
					.setProperty(
							RepoPropsConstant.PC_CLIENT_INSTALL_PATH,
							properties
									.getProperty(RepoPropsConstant.PC_CLIENT_INSTALL_PATH));
			rep.getProperties().setProperty(
					RepoPropsConstant.TARGET_FOLDER_NAME,
					properties
							.getProperty(RepoPropsConstant.TARGET_FOLDER_NAME));
			rep.getProperties().setProperty(RepoPropsConstant.TARGET_REPO_NAME,
					properties.getProperty(RepoPropsConstant.TARGET_REPO_NAME));
			rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_HOST,
					properties.getProperty(RepoPropsConstant.REPO_SERVER_HOST));
			rep.getProperties().setProperty(RepoPropsConstant.ADMIN_PASSWORD,
					properties.getProperty(RepoPropsConstant.ADMIN_PASSWORD));
			rep.getProperties().setProperty(RepoPropsConstant.ADMIN_USERNAME,
					properties.getProperty(RepoPropsConstant.ADMIN_USERNAME));
			rep.getProperties().setProperty(RepoPropsConstant.REPO_SERVER_PORT,
					properties.getProperty(RepoPropsConstant.REPO_SERVER_PORT));
			rep.getProperties().setProperty(RepoPropsConstant.SERVER_PORT,
					properties.getProperty(RepoPropsConstant.SERVER_PORT));
			rep.getProperties().setProperty(RepoPropsConstant.DATABASETYPE,
					properties.getProperty(RepoPropsConstant.DATABASETYPE));
			if (properties.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER) != null)
				rep
						.getProperties()
						.setProperty(
								RepoPropsConstant.PMREP_CACHE_FOLDER,
								properties
										.getProperty(RepoPropsConstant.PMREP_CACHE_FOLDER));
		} else {
			throw new IOException("pcconfig.properties file not found.");
		}
	}

	protected void createRepository() {
		rep = new Repository("PowerCenter", "PowerCenter",
				"This repository contains API test samples");
	}

	public void setRepository(Repository rep) {
		this.rep = rep;
	}

	protected void setMapFileName(String filename) {
		StringBuffer buff = new StringBuffer();
		buff.append(System.getProperty("user.dir"));
		buff.append(java.io.File.separatorChar);
		buff.append(filename);
		buff.append(".xml");
		mapFileName = buff.toString();
	}

	synchronized public void setRepositoryConnectionManager(
			RepositoryConnectionManager repMgr) {
		if (repMgr == null)
			this.repmgr = new CachedRepositoryConnectionManager(
					new PmrepRepositoryConnectionManager());
		else {
			this.repmgr = repMgr;
		}
		rep.setRepositoryConnectionManager(repmgr);
	}

	synchronized public void execute() throws Exception {

		/*
		 *  retrieve a folder. In this case example I have a folder called temp in my repository that I am retrieving
		 *  Change the condition appropriately to retrieve any other folder
		 */
		Vector folders = rep.getFolder(new NameFilter() {
			public boolean accept(String name) {
				return name.equals("temp");
			}
		});

		/*
		 * The cache is updated with the first call to CacheRepositoryConnectionManager. The list of folders are retrieved from
		 * from the cache. Subsequent filtered calls to getFolder will retrieve the folder from the cache without making a call to the repository. 
		 * 
		 */
		Folder temp = (Folder) folders.elementAt(0);
		
		/*Vector vMapplets = temp.getMapplets(new NameFilter() {
			public boolean accept(String name) {
				return name.equals("MappletSample");
			}}
			);*/
		Vector vMapplets = temp.getMapplets();
		Iterator iter = vMapplets.iterator();
		while (iter.hasNext()) {
			
			Mapplet map = (Mapplet) iter.next();
			/*
			 * get the required mapplet
			 */
			if (map.getName().equals("MappletSample")) {
				m_mapplet=map;
				m_mapplet.setModified(true);
			}
		}
		Vector modFolders = rep.getModifiedFolders();
		System.out.println(modFolders.toString());
		generateOutput();
	}

	/**
	 * This method generates the output xml
	 * @throws Exception exception
	 */
	synchronized public void generateOutput() throws Exception {
		MapFwkOutputContext outputContext = new MapFwkOutputContext(
				MapFwkOutputContext.OUTPUT_FORMAT_XML,
				MapFwkOutputContext.OUTPUT_TARGET_FILE, mapFileName);

		//rep.saveAndImportModifiedFolders(outputContext);
		rep.save(outputContext, false);
	}
	
	protected void genrateXML() throws Exception
	{
		folder.addMapplet(m_mapplet);
		 MapFwkOutputContext outputContext = new MapFwkOutputContext(
	                MapFwkOutputContext.OUTPUT_FORMAT_XML, MapFwkOutputContext.OUTPUT_TARGET_FILE,
					mapFileName);

	        try {
	            intializeLocalProps();
	        }
	        catch (IOException ioExcp) {
	            System.err.println( "Error reading pcconfig.properties file." );
	            System.err.println( "The properties file should be in directory where Mapping Framework is installed.");
	            System.exit( 0 );
	        }

	        boolean doImport = false;
	        if (runMode == 1) doImport = true;
	        rep.save(outputContext, doImport);
	        System.out.println( "Mapping generated in " + mapFileName );       
	}
	
	
	 protected void intializeLocalProps() throws IOException {

	        Properties properties = new Properties();
	       
	        //InputStream propStream = new FileInputStream(filename);
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



	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			MappletReader repo = new MappletReader();
			RepositoryConnectionManager repMgr = new CachedRepositoryConnectionManager(
					new PmrepRepositoryConnectionManager());
			repo.setMapFileName("MappletReaderSample");
			repo.setRepositoryConnectionManager(repMgr);
			repo.execute();
			//repo.genrateXML();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}

	}

}

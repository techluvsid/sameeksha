/*
 * MappletStandAlone.java Created on Oct 26, 2007.
 *
 * Copyright 2007 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * rjain
 */

package com.informatica.powercenter.sdk.mapfwk.samples;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;
import java.util.Iterator;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;

/**
 * This class is for creating a mapplet which is having filter transformation and lookup transformation
 * This will create a stand alone mapplet, which could be imprted in repository
 * @author rjain
 *
 */
public class MappletStandAloneSample {
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    private Repository rep;
    private Folder folder;
    private String mapFileName;
    private int runMode = 0;

    private Source manufacturerSrc;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    private void createMapplet() throws Exception {
        //creating mapplet object
        Mapplet mapplet = new Mapplet("MappletLookup", "MappletLookup",
                "This is a test for Lookup mapplet");
        
        //setting mapplet file name
        setMapFileName(mapplet);

        // create helper for mapplet
        TransformHelper helper = new TransformHelper(mapplet);

        //creating an inputTransformation
        OutputSet outputSet = helper.inputTransform(getItemsSourceRs(), "InputTransform");
        RowSet inputRS = (RowSet) outputSet.getRowSets().get(0);
        
        //create a filterTransformation
        RowSet FilterRS=(RowSet)helper.filter(inputRS, "TRUE", "FilereTrans").getRowSets().get(0);
        
        PortPropagationContext filterRSContext = PortPropagationContextFactory
                .getContextForExcludeColsFromAll(new String[] { "Manufacturer_Id" });

        // create a lookup transformation
        outputSet = helper.lookup(FilterRS, manufacturerSrc,
                "manufacturer_id = in_manufacturer_id",
                "Lookup_Manufacturer_Table");
        RowSet lookupRS = (RowSet) outputSet.getRowSets().get(0);
        
        PortPropagationContext lkpRSContext = PortPropagationContextFactory
                .getContextForIncludeCols(new String[] { "Manufacturer_Name" });

        Vector vInputSets = new Vector();
        vInputSets.add(new InputSet(FilterRS, filterRSContext)); // remove
                                                            // Manufacturer_id
        vInputSets.add(new InputSet(lookupRS, lkpRSContext)); // propagate
                                                                // only
                                                                // Manufacturer_Name
        //creating outputTransformation
        helper.outputTransform(vInputSets, "OutputTRansform");
        
        //adding this mapplet to folder
        folder.addMapplet(mapplet);
        
        
    }

 
    
    private RowSet getItemsSourceRs() {
        Vector fields = new Vector();
        Field field1 = new Field( "ItemId", "ItemId", "", DataTypeConstants.INTEGER, "10", "0",
                    FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
        fields.add( field1 );

        Field field2 = new Field( "Item_Name", "Item_Name", "", DataTypeConstants.STRING, "72", "0",
                    FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field2 );

        Field field3 = new Field( "Item_Desc", "Item_Desc", "", DataTypeConstants.STRING, "72", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field3 );

        Field field4 = new Field( "Price", "Price", "", DataTypeConstants.DECIMAL, "10", "2",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field4 );

        Field field5 = new Field( "Wholesale_cost", "Wholesale_cost", "", DataTypeConstants.DECIMAL, "10", "2",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field5 );

        Field field6 = new Field( "Manufacturer_id", "Manufacturer_id", "", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field6 );

        RowSet rs= new RowSet();
        rs.add(fields);
        return rs;

    }

    public boolean validateRunMode(String value) {
        int val = Integer.parseInt(value);
        if ( val > 1 || val < 0) {
           printUsage();
           return false;
        } else {
           runMode = val;
           return true;
        }
    }
    
    public void printUsage(){
        String errorMsg = "***************** USAGE *************************\n";
        errorMsg += "\n";
        errorMsg += "Valid arguments are:\n";
        errorMsg += "0       => Generate powermart xml file\n";
        errorMsg += "1       => Import powermart xml file into PowerCenter Repository\n";        
        errorMsg += "\n";
        errorMsg += "********************************************************\n";
        System.out.println(errorMsg);
    }

    private void setMapFileName( Mapplet mapplet ) {
        StringBuffer buff = new StringBuffer();
        buff.append( System.getProperty( "user.dir" ) );
        buff.append( java.io.File.separatorChar );
        buff.append( mapplet.getName() );
        buff.append( "mapplet.xml" );
        mapFileName = buff.toString();
    }
    
    /**
     * Common execute method
     */
    public void execute() throws Exception {
        init();
        createMapplet();
        
        generateOutput();

    }
    
    /**
     * Initialize the method
     */
    private void init() {
        createRepository();
        createFolder();
        createSources();
    }

    private void createRepository() {
        rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );
    }
    
    /**
     * Creates a folder
     */
    private void createFolder() {
        folder = new Folder( "JavaMappletSamples", "JavaMappletSamples", "This is a folder containing java mapplet samples" );
        rep.addFolder( folder );
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    private void createSources() {
        manufacturerSrc = this.createManufacturersSource();
        folder.addSource(manufacturerSrc);
       
    }

    /**
     * This method generates the output xml
     * @throws Exception exception
     */
    public void generateOutput() throws Exception {
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
    
    private void intializeLocalProps() throws IOException {

        Properties properties = new Properties();
        // String filename = "C:\\OUTPUT\\eBiz\\javamappingsdk_Output\\pcconfig.properties";
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
     * This method creates the source for manufacturers table
     *
     * @return Source object
     */
    private Source createManufacturersSource() {
        Source manufacturerSource;
        Vector fields = new Vector();
        Field field1 = new Field( "Manufacturer_Id", "Manufacturer_Id", "", DataTypeConstants.INTEGER, "10", "0",
                    FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
        fields.add( field1 );

        Field field2 = new Field( "Manufacturer_Name", "Manufacturer_Name", "", DataTypeConstants.STRING, "72", "0",
                    FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add( field2 );

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Manufacturer.csv");
        manufacturerSource = new Source( "Manufacturers", "Manufacturers", "This is Manufacturers table", "Manufacturers", info);
        manufacturerSource.setFields( fields );
        return manufacturerSource;
    }
    
    private ConnectionInfo getFlatFileConnectionInfo() {

        ConnectionInfo infoProps = new ConnectionInfo( SourceTargetTypes.FLATFILE_TYPE );
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_SKIPROWS,"1");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_DELIMITERS,";");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.DATETIME_FORMAT,"A  21 yyyy/mm/dd hh24:mi:ss");
        infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_QUOTE_CHARACTER,"DOUBLE");

    return infoProps;

 }

     public static void main(String args[]) {
        try {
            MappletStandAloneSample mappSample = new MappletStandAloneSample();
            if (args.length > 0) {
                if (mappSample.validateRunMode(args[0])) {
                    mappSample.execute();
                }
            } else {
                mappSample.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception is: " + e.getMessage());
        }
    }

}

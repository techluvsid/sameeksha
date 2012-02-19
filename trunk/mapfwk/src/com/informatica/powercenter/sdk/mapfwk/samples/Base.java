/*
 * Base.java Created on Nov 4, 2005.
 *
 * Copyright 2004 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.informatica.powercenter.sdk.mapfwk.samples;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.RepoPropsConstant;
import com.informatica.powercenter.sdk.mapfwk.core.Repository;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTargetTypes;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;

/**
 *
 *
 */
public abstract class Base {
	/////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	/////////////////////////////////////////////////////////////////////////////////////
    protected Repository rep;
    protected Folder folder;
    protected Session session;
    protected Workflow workflow;
    protected String mapFileName;
    protected Mapping mapping;
    protected int runMode = 0;


    /**
     * Common execute method
     */
    public void execute() throws Exception {
    	init();
    	createMappings();
    	createSession();
    	createWorkflow();
    	generateOutput();

    }

    /**
     * Initialize the method
     */
    protected void init() {
    	createRepository();
    	createFolder();
    	createSources();
    	createTargets();
    }

    /**
     * Create a repository
     */
    protected void createRepository() {
    	rep = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );
    }

    /**
     * Creates a folder
     */
    protected void createFolder() {
    	folder = new Folder( "JavaMappingSamples", "JavaMappingSamples", "This is a folder containing java mapping samples" );
    	rep.addFolder( folder );
    }

    /**
     * Create sources
     */
    protected abstract void createSources(); // override in base class to create appropriate sources

    /**
     * Create targets
     */
    protected abstract void createTargets(); // override in base class to create appropriate targets

    /**
     * Creates a mapping
     * It needs to be overriddden for the sample
     * @return Mapping
     */
    protected abstract void createMappings() throws Exception; // override in base class

    /**
     * Create session
     */
    protected abstract void createSession() throws Exception;

    /**
     * Create workflow
     */
    protected abstract void createWorkflow() throws Exception;

    /**
     * Create source for Employee Source
     */
    protected Source createEmployeeSource() {
        Vector fields = new Vector();
        Field field1 = new Field("EmployeeID", "EmployeeID","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true);
        fields.add(field1);

        Field field2 = new Field("LastName", "LastName","", DataTypeConstants.STRING, "20", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field2);

        Field field3 = new Field("FirstName", "FirstName","", DataTypeConstants.STRING, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field3);

        Field field4 = new Field("Title", "Title","", DataTypeConstants.STRING, "30",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field4);

        Field field5 = new Field("TitleOfCourtesy", "TitleOfCourtesy","", DataTypeConstants.STRING, "25", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field5);

        Field field6 = new Field("BirthDate", "BirthDate", "",DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field6);

        Field field7 = new Field("HireDate", "HireDate","", DataTypeConstants.DATE, "19",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field7);

        Field field8 = new Field("Address", "Address","", DataTypeConstants.STRING, "60", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field8);

        Field field9 = new Field("City", "City","", DataTypeConstants.STRING,
                "15", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field9);

        Field field10 = new Field("Region", "Region","", DataTypeConstants.STRING,
                "15", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field10);

        Field field11 = new Field("PostalCode", "PostalCode","", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field11);

        Field field12 = new Field("Country", "Country","", DataTypeConstants.STRING,
                "15", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field12);

        Field field13 = new Field("HomePhone", "HomePhone","", DataTypeConstants.STRING,
                "24", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field13);

        Field field14 = new Field("Extension", "Extension","", DataTypeConstants.STRING,
                "4", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field14);

        Field field15 = new Field("Notes", "Notes","", DataTypeConstants.CLOB,
                "350", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field15);

        Field field16 = new Field("ReportsTo", "ReportsTo","", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field16);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Employees.csv");
        Source employeeSource = new Source( "Employee", "Employee", "This is Employee Table", "Employee", info );
        employeeSource.setFields( fields );
        return employeeSource;
    }


    /**
     * Create source for Employee Source
     */
    protected Source createOrderDetailSource() {
        Vector fields = new Vector();
        Field field1 = new Field("OrderID", "OrderID","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field1);

        Field field2 = new Field("ProductID", "ProductID","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field2);

        Field field3 = new Field("UnitPrice", "UnitPrice","", DataTypeConstants.DECIMAL, "28", "4",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field3);

        Field field4 = new Field("Quantity", "Quantity","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field4);

        Field field5 = new Field("Discount", "Discount","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field5);

        Field field6 = new Field("VarcharFld", "VarcharFld","", DataTypeConstants.VARCHAR, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field6);

        Field field7 = new Field("Varchar2Fld", "Varchar2Fld","", DataTypeConstants.VARCHAR2, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field7);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Order_Details.csv");
        Source ordDetailSource = new Source( "OrderDetail", "OrderDetail", "This is Order Detail Table", "OrderDetail", info );
        ordDetailSource.setFields( fields );
        return ordDetailSource;
    }

    /**
     * Create source for Items table
     */
    protected Source createItemsSource() {
    	Source itemsSource;
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

    	ConnectionInfo info = getFlatFileConnectionInfo();
    	info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"items.csv");
    	itemsSource = new Source( "Items", "Items", "This is Items table", "Items", info);
    	itemsSource.setFields( fields );
    	return itemsSource;
    }

    /**
     * Create source for Products table
     */
    protected Source createProductsSource() {
    	Source productSource;
    	Vector fields = new Vector();

       	Field foriegnfield = new Field( "ItemId", "ItemId", "", DataTypeConstants.INTEGER, "10", "0",
				FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false );
        foriegnfield.setReferenceConstraint("Items", "ItemId");
	    fields.add( foriegnfield );

    	Field field1 = new Field( "Item_No", "Item_No", "", DataTypeConstants.INTEGER, "10", "0",
    				FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
    	fields.add( field1 );

    	Field field2 = new Field( "Item_Name", "Item_Name", "", DataTypeConstants.STRING, "72", "0",
    				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
    	fields.add( field2 );

    	Field field3 = new Field( "Item_Desc", "Item_Desc", "", DataTypeConstants.STRING, "72", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
    	fields.add( field3 );

    	Field field4 = new Field( "Cust_Price", "Cust_Price", "", DataTypeConstants.DECIMAL, "10", "2",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
    	fields.add( field4 );

    	Field field5 = new Field( "Product_Category", "Product_Category", "", DataTypeConstants.STRING, "30", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
    	fields.add( field5 );


    	ConnectionInfo info = getFlatFileConnectionInfo();
    	info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"products.csv");
    	productSource = new Source( "Products", "Products", "This is products table", "Products", info);
    	productSource.setFields( fields );
    	return productSource;
    }


    /**
     * This method creates the source for manufacturers table
     *
     * @return Source object
     */
    protected Source createManufacturersSource() {
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

    /**
     * Create Orders Source
     * @return
     */
    protected Source createOrdersSource() {
    	Source ordersSource;
    	Vector fields = new Vector();
        Field field1 = new Field("OrderID", "OrderID","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true);
        fields.add(field1);

        Field field2 = new Field("CustomerID", "Customer","", DataTypeConstants.STRING, "5", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field2);

        Field field3 = new Field("EmployeeID", "EmployeeID","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field3);

        Field field5 = new Field("OrderDate", "OrderDate", "",DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field5);

        Field field6 = new Field("RequiredDate", "RequiredDate", "",DataTypeConstants.DATE, "19", "0",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field6);

        Field field7 = new Field("ShippedDate", "ShippedDate","", DataTypeConstants.DATE, "19",
                "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field7);

        Field field8 = new Field("ShipVia", "ShipVia","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field8);

        Field field9 = new Field("Freight", "Freight","", DataTypeConstants.DECIMAL, "28", "4",
                FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field9);

        Field field10 = new Field("ShipName", "ShipName", "", DataTypeConstants.STRING, "40", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field10);

        Field field11 = new Field("ShipAddress", "ShipAddress", "", DataTypeConstants.STRING, "60", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field11);

        Field field12 = new Field("ShipCity", "ShipCity", "", DataTypeConstants.STRING, "15", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field12);

        Field field13 = new Field("ShipRegion", "ShipRegion", "", DataTypeConstants.STRING, "15", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field13);

        Field field14 = new Field("ShipPostalCode", "ShipPostalCode", "", DataTypeConstants.STRING, "10", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field14);

        Field field15 = new Field("ShipCountry", "ShipCountry", "", DataTypeConstants.STRING, "15", "",
        		FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
        fields.add(field15);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Orders.csv");
        ordersSource = new Source( "Orders", "Orders", "This is Orders table", "Orders", info );
    	ordersSource.setFields( fields );
    	return ordersSource;
    }

	/**
	 * Method to create job table source for Oracle
	 */
	protected Source createJobSource() {
		Source jobSource = null;

		Vector vFields = new Vector();
		Field jobIDField = new Field( "JOB_ID", "JOB_ID", "",
					DataTypeConstants.VARCHAR2, "10", "0",
    				FieldConstants.PRIMARY_KEY, Field.FIELDTYPE_SOURCE, true );
    	vFields.add( jobIDField );

		Field jobTitleField = new Field( "JOB_TITLE", "JOB_TITLE", "",
				DataTypeConstants.VARCHAR2, "35", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( jobTitleField );

		Field minSalField = new Field( "MIN_SALARY", "MIN_SALARY", "",
				DataTypeConstants.DECIMAL, "6", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( minSalField );

		Field maxSalField = new Field( "MAX_SALARY", "MAX_SALARY", "",
				DataTypeConstants.DECIMAL, "6", "0",
				FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE, false );
		vFields.add( maxSalField );

		ConnectionInfo info = getRelationalConnInfo( SourceTargetTypes.RELATIONAL_TYPE_ORACLE, "Oracle_Source" );
		jobSource = new Source( "JOBS", "JOBS", "This is JOBS table", "JOBS", info );
		jobSource.setFields(vFields);
		return jobSource;
	}

	protected ConnectionInfo getRelationalConnInfo( int dbType, String dbName ) {
		ConnectionInfo connInfo = null;
		connInfo = new ConnectionInfo( dbType );
		connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, dbName );
		return connInfo;
	}

	/**
	 * Method to create relational target
	 */
	protected Target createRelationalTarget( int type, String name ) {
		Target target = new Target(name,
				name, name,
				name, new ConnectionInfo( type ) ) ;
		return target;
	}
    /**
     * This method creates the target for the mapping
     * @return
     */
    public Target createFlatFileTarget( String name ) {
        Target tgt = new Target(name, name, "", "",new ConnectionInfo( SourceTargetTypes.FLATFILE_TYPE ));
        tgt.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_CODEPAGE, "MS1252");
        tgt.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.OUTPUT_FILENAME, name+".out");
        return tgt;
    }

    /**
     * This method creates the target for relational database
     * @param name
     * @param relationalType
     * @return
     */
    public Target createRelationalTarget( String name, int relationalType ) {
        Target tgt = new Target(name, name, "", "",new ConnectionInfo( relationalType ));
        return tgt;
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

    protected void setMapFileName( Mapping mapping ) {
    	StringBuffer buff = new StringBuffer();
    	buff.append( System.getProperty( "user.dir" ) );
    	buff.append( java.io.File.separatorChar );
    	buff.append( mapping.getName() );
    	buff.append( ".xml" );
    	mapFileName = buff.toString();
    }

    protected void addFieldsToSource( Source src, Vector vFields ) {
    	int size = vFields.size();
    	for ( int i = 0; i < size; i++ ) {
    		src.addField( (Field)vFields.get( i ) );
    	}
    }

    protected ConnectionInfo getFlatFileConnectionInfo() {

           ConnectionInfo infoProps = new ConnectionInfo( SourceTargetTypes.FLATFILE_TYPE );
           infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_SKIPROWS,"1");
           infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_DELIMITERS,";");
           infoProps.getConnProps().setProperty(ConnectionPropsConstants.DATETIME_FORMAT,"A  21 yyyy/mm/dd hh24:mi:ss");
           infoProps.getConnProps().setProperty(ConnectionPropsConstants.FLATFILE_QUOTE_CHARACTER,"DOUBLE");

    	return infoProps;

    }

    protected ConnectionInfo getRelationalConnectionInfo(int dbType) {
           ConnectionInfo infoProps = new ConnectionInfo( dbType );
           return infoProps;
    }
	/**
	 * Method to get relational connection info object
	 */
	protected ConnectionInfo getRelationalConnectionInfo( int dbType, String dbName ) {
		ConnectionInfo connInfo =  new ConnectionInfo( dbType );
		connInfo.getConnProps().setProperty( ConnectionPropsConstants.DBNAME, dbName );
		return connInfo;
	}

    protected void intializeLocalProps() throws IOException {

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

}

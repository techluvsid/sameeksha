/*
 * MappletSample.java Created on Oct 26, 2007.
 *
 * Copyright 2007 Informatica Corporation. All rights reserved.
 * INFORMATICA PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * rjain
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Vector;
import java.util.Iterator;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.Mapplet;

/**
 * This will create an mapplet having have three pipelines, each doing some transforamtion. Then this mapplet has 
 * been used in a mapping
 * @author rjain
 *
 */
public class MappletSample extends Base
{
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source ageSrc;

    protected Source idPostSrc;

    protected Source idSrc;

    protected Source nameSrc;

    protected Target ageTrg;

    protected Target fullNameTrg;

    protected Target idPostTrg;

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createMappings()
     */
    protected void createMappings() throws Exception
    {
        //creating a mapplet obeject
        Mapplet mapplet = new Mapplet("MappletSample", "MappletSample",
                "This is a MappletSample mapplet");

        // create helper for mapplet
        TransformHelper helperMapplet = new TransformHelper(mapplet);
        
        //creating mapplet transformations
        createFirstPipeline(helperMapplet);
        createSecondPipeline(helperMapplet);
        createthirdPipeline(helperMapplet);
        
        //adding this mapplet to folder
        folder.addMapplet(mapplet);
        
        //creating mapping object that will use above created mapplet
        mapping = new Mapping("MappletSampleMapping", "MappletSampleMapping",
                "This is a sample for mapplet mapping");
        
        //setting mapping file name
        setMapFileName(mapping);

        // create helper for mapping..
        TransformHelper helperMapping = new TransformHelper(mapping);
       
        //creating source qualifier
        OutputSet outputSet = helperMapping.sourceQualifier(idSrc);
        RowSet dsqIdRS = (RowSet) outputSet.getRowSets().get(0);
        
        outputSet=helperMapping.sourceQualifier(nameSrc);
        RowSet dsqNameRS=(RowSet) outputSet.getRowSets().get(0);

        //creating vector of InputSet that will be used for creating mapplet tranformation
        Vector vInSets = new Vector(); 
        vInSets.add(new InputSet(dsqIdRS));
        vInSets.add(new InputSet(dsqNameRS));

        //creating mapplet transformation
       outputSet = helperMapping.mapplet(mapplet, vInSets,
                "myMapplet");
        
        Vector vMappRS = outputSet.getRowSets();
       
        
        //  write to target
        mapping.writeTarget((RowSet) vMappRS.get(0), idPostTrg);
        mapping.writeTarget((RowSet) vMappRS.get(1), ageTrg);
        mapping.writeTarget((RowSet) vMappRS.get(2), fullNameTrg);
       
        //adding mapping object to folder
        folder.addMapping(mapping);

    }
    
    
    private void createFirstPipeline(TransformHelper helperMapplet) throws Exception
    {
        OutputSet outputSet = helperMapplet.inputTransform(getIdRs(),
                "InputIDTransform");
        RowSet inputIDRS = (RowSet) outputSet.getRowSets().get(0);

        RowSet FilterRS = (RowSet) helperMapplet.filter(inputIDRS, "TRUE",
                "FilereTrans").getRowSets().get(0);

        PortPropagationContext filterRSContext = PortPropagationContextFactory
                .getContextForAllIncludeCols();

        // create a lookup transformation
        outputSet = helperMapplet.lookup(FilterRS, idPostSrc, "ID = IN_ID",
                "Lookup_IdPost_Table");
        RowSet lookupRS = (RowSet) outputSet.getRowSets().get(0);

        PortPropagationContext lkpRSContext = PortPropagationContextFactory
                .getContextForIncludeCols(new String[] { "post" });

        Vector vInputSets = new Vector();
        vInputSets.add(new InputSet(FilterRS, filterRSContext));

        vInputSets.add(new InputSet(lookupRS, lkpRSContext));

        helperMapplet.outputTransform(vInputSets, "outputIdPost");

    }

    private void createSecondPipeline(TransformHelper helperMapplet) throws Exception
    {
        //creating an inputTransformation 
        OutputSet outputSet = helperMapplet.inputTransform(getNameRs(),
        "InputNameTransform");
        RowSet inputNameRS = (RowSet) outputSet.getRowSets().get(0);
        
        String expr="string(10, 0) fullname = name";
        TransformField fullName=new TransformField(expr);
       
        //creating an expression transformation
        outputSet=helperMapplet.expression(inputNameRS, fullName, "ExpressionTrans");
        RowSet exprRS=(RowSet)outputSet.getRowSets().get(0);
        
        //creating an outputTranformation
        helperMapplet.outputTransform(exprRS, "outputName");  
        
    }
    
    private void createthirdPipeline(TransformHelper helperMapplet) throws Exception
    {
        OutputSet outputSet = helperMapplet.sourceQualifier(ageSrc);
        RowSet dsqRS = (RowSet) outputSet.getRowSets().get(0);
        
        helperMapplet.outputTransform(dsqRS, "outputAge");
        
    }
    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSession()
     */
    protected void createSession() throws Exception
    {
        session = new Session("Session_For_MappletSample", "Session_For_MappletSample",
                "This is session for MappletSample");
        session.setMapping(mapping);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createSources()
     */
    protected void createSources() {
        ageSrc=this.createAgeSource();
        folder.addSource(ageSrc);
        idPostSrc=this.createIdPostSource();
        folder.addSource(idPostSrc);
        idSrc=this.createIdSource();
        folder.addSource(idSrc);
        nameSrc=this.createNameSource();
        folder.addSource(nameSrc);
    }

    protected Source createAgeSource()
    {
        Source ageSrc;
        Vector fields = new Vector();
        Field field1 = new Field("age", "age", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(
                ConnectionPropsConstants.SOURCE_FILENAME, "age.csv");
        ageSrc = new Source("age", "age", "This is age table", "AgeSrc", info);
        ageSrc.setFields(fields);
        return ageSrc;
    }

    protected Source createIdPostSource()
    {
        Source idPostSrc;
        Vector fields = new Vector();
        Field field1 = new Field("ID", "ID", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);
        
        Field field2 = new Field("post", "post", "", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field2);
        

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(
                ConnectionPropsConstants.SOURCE_FILENAME, "idPost.csv");
        idPostSrc = new Source("idPost", "idPost", "This is idPost table", "IdPostSrc", info);
        idPostSrc.setFields(fields);
        return idPostSrc;
        
    }
    
    protected Source createIdSource()
    {
        Source idSrc;
        Vector fields = new Vector();
        Field field1 = new Field("ID", "ID", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(
                ConnectionPropsConstants.SOURCE_FILENAME, "id.csv");
        idSrc = new Source("ID", "ID", "This is id table", "IDSrc", info);
        idSrc.setFields(fields);
        return idSrc;
    }

    protected Source createNameSource()
    {
        Source nameSrc;
        Vector fields = new Vector();
        Field field1 = new Field("name", "name", "", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);

        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(
                ConnectionPropsConstants.SOURCE_FILENAME, "name.csv");
        nameSrc = new Source("name", "name", "This is name table", "nameSrc", info);
        nameSrc.setFields(fields);
        return nameSrc;
    }


    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createTargets()
     */
    protected void createTargets()
    {
        ageTrg=this.createFlatFileTarget("age");

        fullNameTrg=this.createFlatFileTarget("fullName");

        idPostTrg=this.createFlatFileTarget("idPost");

    }
    
    protected RowSet getIdRs()
    {
        Vector fields = new Vector();
        Field field1 = new Field("ID", "ID", "", DataTypeConstants.INTEGER,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);
        
        RowSet rs=new RowSet();
        rs.setFields(fields);
        return rs;
        
    }
    
    protected RowSet getNameRs()
    {
        Vector fields = new Vector();
        Field field1 = new Field("name", "name", "", DataTypeConstants.STRING,
                "10", "0", FieldConstants.NOT_A_KEY, Field.FIELDTYPE_SOURCE,
                true);
        fields.add(field1);
        
        RowSet rs=new RowSet();
        rs.setFields(fields);
        return rs;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.informatica.powercenter.sdk.mapfwk.samples.Base#createWorkflow()
     */
    protected void createWorkflow() throws Exception
    {
        workflow = new Workflow("Workflow_for_MappletSample", "Workflow_for_MappletSample",
                "This workflow for mapplet sample");
        workflow.addSession(session);
        folder.addWorkFlow(workflow);

    }

    public static void main(String args[])
    {
        try
        {
            MappletSample mappletSample = new MappletSample();
            if (args.length > 0)
            {
                if (mappletSample.validateRunMode(args[0]))
                {
                    mappletSample.execute();
                    
                }
            } else
            {
                mappletSample.printUsage();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            System.err.println("Exception is: " + e.getMessage());
        }
    }

}

/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Iterator;
import java.util.Vector;

import com.informatica.powercenter.sdk.mapfwk.core.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.DataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldConstants;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerField;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerRecord;
import com.informatica.powercenter.sdk.mapfwk.core.NormalizerTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.core.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.core.PowerCenterCompatibilityFactory;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;


/**
 * 
 * @author rjain
 *
 */

public class NormalizerMultiGroupExample extends Base
{
    // /////////////////////////////////////////////////////////////////////////////////////
    // Instance variables
    // /////////////////////////////////////////////////////////////////////////////////////
    protected Source simpleSource;

    protected Target outputTarget;

    /**
     * Create sources
     */
    protected void createSources() {
        simpleSource = this.simpleSource();
        folder.addSource(simpleSource);
    }

    protected Source simpleSource()
    {
        Vector fields = new Vector();
        Field field1 = new Field("f1", "f1","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field1);
        
        Field field2 = new Field("f2", "f2","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field2);
        Field field3 = new Field("f3", "f3","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field3);
        Field field4 = new Field("f4", "f4","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field4);
        Field field5 = new Field("f5", "f5","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field5);
        Field field6 = new Field("f6", "f6","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field6);
        Field field7 = new Field("f7", "f7","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field7);

        Field field8 = new Field("f8", "f8","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field8);
        Field field9 = new Field("f9", "f9","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field9);
        Field field10 = new Field("f10", "f10","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field10);
        Field field11 = new Field("f11", "f11","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field11);
        Field field12 = new Field("f12", "f12","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field12);
        Field field13 = new Field("f13", "f13","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field13);
        Field field14 = new Field("f14", "f14","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field14);
        Field field15 = new Field("f15", "f15","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field15);
        Field field16 = new Field("f16", "f16","", DataTypeConstants.INTEGER, "10", "0",
                FieldConstants.FOREIGN_KEY, Field.FIELDTYPE_SOURCE, false);
        fields.add(field16);
                
        
        ConnectionInfo info = getFlatFileConnectionInfo();
        info.getConnProps().setProperty(ConnectionPropsConstants.SOURCE_FILENAME,"Order_Details.csv");
        Source ordDetailSource = new Source( "OrderDetail", "OrderDetail", "This is Order Detail Table", "OrderDetail", info );
        ordDetailSource.setFields( fields );
        return ordDetailSource;        
    }
    /**
     * Create targets
     */
    protected void createTargets() {
        outputTarget = this.createFlatFileTarget("Normalizer_Output");
    }

    public void createMappings() throws Exception {
        // create a mapping
        mapping = new Mapping("NormalizerMultiGroupMapping", "NormalizerMultiGroupMapping",
                "This is sample for Normalizer");
        setMapFileName(mapping);
        TransformHelper helper = new TransformHelper(mapping);

        // creating DSQ Transformation
        RowSet dsqRS = (RowSet) helper.sourceQualifier(this.simpleSource)
                .getRowSets().get(0);
        
        Field field1 = new Field("f1","","",DataTypeConstants.NUMBER,"10","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        NormalizerField nField1=new NormalizerField("f1","","","3",field1);
        Field field2 = new Field("f2","","",DataTypeConstants.NUMBER,"10","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        NormalizerField nField2=new NormalizerField("f2","","","0",field2);
        
        
        Vector vNFields1 = new Vector();
        vNFields1.add(nField1);
        vNFields1.add(nField2);
        
        
        NormalizerRecord record3 = new NormalizerRecord("record3","record3","record3","2", null,vNFields1);
        Vector vRecords3=new Vector();
        vRecords3.add(record3);
        NormalizerRecord record2 = new NormalizerRecord("record2","record2","record2","0", vRecords3,null);
        Vector vRecords2=new Vector();
        vRecords2.add(record2);
        NormalizerRecord record1 = new NormalizerRecord("record1","record1","record1","2", vRecords2,null);
        
        Vector vRecords=new Vector();
        vRecords.add(record1);
        
        
        Vector vTransformFields = NormalizerTransformation.getNormalizerPorts(vRecords, null);
        Iterator iter = vTransformFields.iterator();
        Vector positions = new Vector();
        while(iter.hasNext())
        {            
            TransformField field = (TransformField) iter.next();            
            if(field.getPortType() == TransformField.TYPE_INPUT)
                positions.add(field.getField());
        }
        PortLinkContext portLinkContext = PortLinkContextFactory.getPortLinkContextByPosition(positions);
        

        InputSet inset = new InputSet (dsqRS, portLinkContext);
        Vector vInsets = new Vector();
        vInsets.add(inset);
        NormalizerTransformation normalizer = new NormalizerTransformation("myNormalizer","myNormalizer","myNormalizer","myNormalizer",vRecords,null,vInsets,vTransformFields,null);
        // write to target
        normalizer.setMapping(mapping);
        mapping.addTransformation(normalizer);
        OutputSet outset = normalizer.apply();
        mapping.writeTarget((RowSet)outset.getRowSets().get(0), this.outputTarget);
        folder.addMapping(mapping);
    }

    /**
     * Create session
     */
    protected void createSession() throws Exception {
        session = new Session("Session_For_Normalizer", "Session_For_Normalizer",
                "This is session for Normalizer");
        session.setMapping(mapping);
    }

    /**
     * Create workflow
     */
    protected void createWorkflow() throws Exception {
        workflow = new Workflow("Workflow_for_Normalizer",
                "Workflow_for_Normalizer", "This workflow for Normalizer");
        workflow.addSession(session);
        folder.addWorkFlow(workflow);
    }

    public static void main(String args[]) {
        try {
            NormalizerMultiGroupExample normalizer = new NormalizerMultiGroupExample();
            if (args.length > 0) {
                if (normalizer.validateRunMode(args[0])) {
                    PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
                    compFactory.setCompatibilityVersion(8, 5,1);
                    normalizer.execute();
                }
            } else {
                normalizer.printUsage();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Exception is: " + e.getMessage());
        }
    }

}

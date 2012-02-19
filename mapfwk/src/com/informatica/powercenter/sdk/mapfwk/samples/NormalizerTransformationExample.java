/**
 * 
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Iterator;
import java.util.Vector;

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
 * @author sramamoo
 *
 */

public class NormalizerTransformationExample extends Base
{
	// /////////////////////////////////////////////////////////////////////////////////////
	// Instance variables
	// /////////////////////////////////////////////////////////////////////////////////////
	protected Source orderDetailSource;

	protected Target outputTarget;

	/**
	 * Create sources
	 */
	protected void createSources() {
		orderDetailSource = this.createOrderDetailSource();
		folder.addSource(orderDetailSource);
	}

	/**
	 * Create targets
	 */
	protected void createTargets() {
		outputTarget = this.createFlatFileTarget("Normalizer_Output");
	}

	public void createMappings() throws Exception {
		// create a mapping
		mapping = new Mapping("NormalizerMapping", "NormalizerMapping",
				"This is sample for Normalizer");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// creating DSQ Transformation
		RowSet dsqRS = (RowSet) helper.sourceQualifier(this.orderDetailSource)
				.getRowSets().get(0);
		
		Field field1 = new Field("field1","","",DataTypeConstants.NUMBER,"10","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
		Field field2 = new Field("field2","","",DataTypeConstants.NSTRING,"20","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
		Field field3 = new Field("field3","","",DataTypeConstants.STRING,"30","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
        NormalizerField nfield1=new NormalizerField("field1","","","0",field1);
        NormalizerField nfield2=new NormalizerField("field2","","","0",field2);
        NormalizerField nfield3=new NormalizerField("field3","","","0",field3);
		Vector vFields = new Vector();
		vFields.add(nfield1);
		vFields.add(nfield2);
		vFields.add(nfield3);
		
		Field field4 = new Field("field4","","",DataTypeConstants.NUMBER,"10","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
		Field field5 = new Field("field5","","",DataTypeConstants.NSTRING,"20","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
		Field field6 = new Field("field6","","",DataTypeConstants.STRING,"30","0",FieldConstants.NOT_A_KEY,Field.FIELDTYPE_TRANSFORM,false);
         NormalizerField nfield4=new NormalizerField("field4","","","0",field4);
         NormalizerField nfield5=new NormalizerField("field5","","","0",field5);
         NormalizerField nfield6=new NormalizerField("field6","","","0",field6);
		Vector vFields1 = new Vector();
		vFields1.add(nfield4);
		vFields1.add(nfield5);
		vFields1.add(nfield6);
		NormalizerRecord record2 = new NormalizerRecord("record2","record2","record2","1", null,vFields);
        Vector vRecords2=new Vector();
        vRecords2.add(record2);
		NormalizerRecord record1 = new NormalizerRecord("record1","record1","record1","0", vRecords2, null);
        Vector vRecords1=new Vector();
        vRecords1.add(record1);
		
		Vector vTransformFields = NormalizerTransformation.getNormalizerPorts(vRecords1, vFields1);
		Iterator iter = vTransformFields.iterator();
		Vector positions = new Vector();
		while(iter.hasNext())
        {            
			TransformField field = (TransformField) iter.next();			
			if(field.getPortType() == TransformField.TYPE_INPUT)
				positions.add(field.getField());
        }
        PortLinkContext portLinkContext = PortLinkContextFactory.getPortLinkContextByPosition(positions);
        
//        InputSet inset = new InputSet (dsqRS, PortPropagationContextFactory.getContextForIncludeCols(new Vector()), portLinkContext);
        InputSet inset = new InputSet (dsqRS, portLinkContext);
        Vector vInsets = new Vector();
        vInsets.add(inset);
		NormalizerTransformation normalizer = new NormalizerTransformation("myNormalizer","myNormalizer","myNormalizer","myNormalizer",vRecords1,vFields1,vInsets,vTransformFields,null);
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
			NormalizerTransformationExample normalizer = new NormalizerTransformationExample();
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

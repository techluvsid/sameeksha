/**
 *
 */
package com.informatica.powercenter.sdk.mapfwk.samples;

import java.util.Date;

import com.informatica.powercenter.sdk.mapfwk.core.Assignment;
import com.informatica.powercenter.sdk.mapfwk.core.Command;
import com.informatica.powercenter.sdk.mapfwk.core.Control;
import com.informatica.powercenter.sdk.mapfwk.core.Decision;
import com.informatica.powercenter.sdk.mapfwk.core.EMail;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PowerMartDataTypeConstants;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Task;
import com.informatica.powercenter.sdk.mapfwk.core.Timer;
import com.informatica.powercenter.sdk.mapfwk.core.TimerType;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.core.WorkflowVariable;

/**
 * @author sramamoo
 *
 */
public class OtherTypesOfTasks extends Base
{
	protected Source orderDetailSource;

	protected Target outputTarget;
	Assignment assignment;
	Control control;
	Decision decision;
	Timer absTimer, relTimer, varTimer;
	Command command;
	EMail email;

	protected void createMappings() throws Exception
	{
		// create a mapping
		mapping = new Mapping("OtherTypesOfTasks", "OtherTypesOfTasks",
				"This is sample for OtherTypesOfTasks");
		setMapFileName(mapping);
		TransformHelper helper = new TransformHelper(mapping);

		// creating DSQ Transformation
		RowSet dsqRS = (RowSet) helper.sourceQualifier(this.orderDetailSource)
				.getRowSets().get(0);

		// create an aggregator Transformation
		// calculate cost per order using the formula
		// SUM((UnitPrice * Quantity) * (100 - Discount1) / 100) grouped by
		// OrderId
		TransformField cost = new TransformField(
				"number(15,0) total_cost = (SUM((UnitPrice * Quantity) * (100 - Discount) / 100))");
		RowSet aggRS = (RowSet) helper.aggregate(dsqRS, cost,
				new String[] { "OrderID" }, "agg_transform").getRowSets()
				.get(0);
		// write to target
		mapping.writeTarget(aggRS, this.outputTarget);

		folder.addMapping(mapping);
	}

	protected void createSession() throws Exception
	{
		session = new Session("Session_For_OtherTypesOfTasks", "Session_For_OtherTypesOfTasks",
		"This is session for OtherTypesOfTasks");
		session.setMapping(mapping);
	}

	protected void createSources()
	{
		orderDetailSource = this.createOrderDetailSource();
		folder.addSource(orderDetailSource);
	}

	protected void createTargets()
	{
		outputTarget = this.createFlatFileTarget("OtherTypesOfTasks_Output");
	}
	private void createTasks()
	{
		assignment = new Assignment("assignment","assignment","This is a test assignment");
		assignment.addAssignmentExpression("$$var1", "1");
		assignment.addAssignmentExpression("$$var2", "$$var1 + 5");
		assignment.addAssignmentExpression("$$var1", "$$var2 - 10");

		control = new Control("control","control","This is a test control");
		control.setControlOption(Control.CONTROL_OPTION_VALUE_ABORT_PARENT);
		assignment.connectToTask(control,"$assignment.ErrorCode != 0");

		decision = new Decision("decision","decision","This is a test decision");
		decision.setDecisionExpression("1 + 2");

		absTimer = new Timer("absTimer","absTimer","absolute timer", TimerType.createAbsoluteTimer(new Date()));
		decision.connectToTask(absTimer);
		
		relTimer = new Timer("relTimer","relTimer","relative timer", TimerType.createRelativeToPreviousTaskTimer(3, 5, 10, TimerType.TIMER_TYPE_START_RELATIVE_TO_TOPLEVEL_WORKFLOW));
		absTimer.connectToTask(relTimer);
		varTimer = new Timer("varTimer","varTimer","variable timer", TimerType.createVariableTimer("$$timerVar"));
		relTimer.connectToTask(varTimer);

		command = new Command("command","command","This is a test command");
		command.addCommand("command1", "ls");
		command.addCommand("command2", "ls -lrt");
		command.addCommand("command1", "df -k .");
		varTimer.connectToTask(command);

		email = new EMail("myEmail","myEmail","my email task");
		email.setEmailUsername("guest@informatica.com");
		email.setEmailSubject("Welcome to Informatica");
		email.setEmailText("This is a test mail");
		command.connectToTask(email);
	}
	protected void createWorkflow() throws Exception
	{
		workflow = new Workflow("Workflow_for_OtherTasks",
				"Workflow_for_OtherTasks", "This workflow for other types of tasks");
		WorkflowVariable wfVar1 = new WorkflowVariable("$$var1",PowerMartDataTypeConstants.INTEGER,"1","var1 ");
		WorkflowVariable wfVar2 = new WorkflowVariable("$$var2",PowerMartDataTypeConstants.INTEGER,"1","var2 ");
		WorkflowVariable wfVar3 = new WorkflowVariable("$$timerVar",PowerMartDataTypeConstants.TIMESTAMP,"","timerVariable ");
		workflow.addWorkflowVariable(wfVar1);
		workflow.addWorkflowVariable(wfVar2);
		workflow.addWorkflowVariable(wfVar3);
		createTasks();
		workflow.addTask(assignment);
		workflow.addTask(control);
		workflow.addTask(decision);
		workflow.addTask(command);
		workflow.addTask(absTimer);
		workflow.addTask(relTimer);
		workflow.addTask(varTimer);
		workflow.addTask(email);
		workflow.addSession(session);

		folder.addWorkFlow(workflow);

	}
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		try {
			OtherTypesOfTasks otherTasks = new OtherTypesOfTasks();
			if (args.length > 0) {
				if (otherTasks.validateRunMode(args[0])) {
					otherTasks.execute();
				}
			} else {
				otherTasks.printUsage();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}
}

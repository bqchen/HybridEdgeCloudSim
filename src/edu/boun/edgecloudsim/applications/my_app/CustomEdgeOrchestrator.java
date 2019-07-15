/*
 * Title:        EdgeCloudSim - Edge Orchestrator
 * 
 * Description: 
 * SampleEdgeOrchestrator offloads tasks to proper server
 * by considering WAN bandwidth and edge server utilization.
 * After the target server is decided, the least loaded VM is selected.
 * If the target server is a remote edge server, MAN is used.
 * 
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 * Copyright (c) 2017, Bogazici University, Istanbul, Turkey
 */

package edu.boun.edgecloudsim.applications.my_app;

import edu.boun.edgecloudsim.cloud_server.CloudVM;
import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_client.CpuUtilizationModel_Custom;
import edu.boun.edgecloudsim.edge_client.Task;
import edu.boun.edgecloudsim.edge_client.mobile_processing_unit.MobileVM;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeVM;
import edu.boun.edgecloudsim.utils.SimLogger;
import edu.boun.edgecloudsim.utils.TaskProperty;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.List;

public class CustomEdgeOrchestrator extends EdgeOrchestrator {
	
	private int numberOfHost; //used by load balancer

	public CustomEdgeOrchestrator(String _policy, String _simScenario) {
		super(_policy, _simScenario);
	}

	@Override
	public void initialize() {
		numberOfHost=SimSettings.getInstance().getNumOfEdgeHosts();
	}

	/*
	 * (non-Javadoc)
	 * @see edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator#getDeviceToOffload(edu.boun.edgecloudsim.edge_client.Task)
	 * 
	 * It is assumed that the edge orchestrator app is running on the edge devices in a distributed manner
	 */
	/**
	 * It will be called by the method {@link CustomMobileDeviceManager#submitTask(TaskProperty)}
	 * helping MobileDeviceManger to decide {nextHopId}
	 */
	@Override
	public int getDeviceToOffload(Task task) {

		int result = 0;
		
		//RODO: return proper host ID

		//dummy task to simulate a task with 1 Mbit file size to upload and download
//		Task dummyTask = new Task(0, 0, 0, 0,
//				128, 128, new UtilizationModelFull(),
//				new UtilizationModelFull(), new UtilizationModelFull());

//		double wanDelay = SimManager.getInstance().getNetworkModel().getUploadDelay(
//				task.getMobileDeviceId(), SimSettings.CLOUD_DATACENTER_ID, dummyTask /* 1 Mbit */);

		double wanDelay = SimManager.getInstance().getNetworkModel().getUploadDelay(
				task.getMobileDeviceId(), SimSettings.CLOUD_DATACENTER_ID, task);

		double wanBW = (wanDelay == 0) ? 0 : (1 / wanDelay); /* Mbps */

		double edgeUtilization = SimManager.getInstance().getEdgeServerManager().getAvgUtilization();

//		double wlanDelay =SimManager.getInstance().getNetworkModel().getUploadDelay(
//				task.getMobileDeviceId(), SimSettings.GENERIC_EDGE_DEVICE_ID, dummyTask);

//		double wlanDelay =SimManager.getInstance().getNetworkModel().getUploadDelay(
//				task.getMobileDeviceId(), SimSettings.GENERIC_EDGE_DEVICE_ID, task);
//
//
//		double wlanBw = (wlanDelay == 0) ? 0 : (1 / wlanDelay); /* Mbps */


		if (policy.equals("ONLY_EDGE")) {
			result =SimSettings.GENERIC_EDGE_DEVICE_ID;
		} else if (policy.equals("ONLY_MOBILE")) {
			result = SimSettings.MOBILE_DATACENTER_ID;
		} else if (policy.equals("HYBRID")) {
			// Todo: modify the policy
			// to decide whether the task should be performed on the mobile, offloaded to the edge device
			// or to the cloud data center


			List<EdgeVM> edgeVMList = SimManager.getInstance().getEdgeServerManager().getVmList(task.getAssociatedHostId());
			double requiredEdgeVmCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(edgeVMList.get(0).getVmType());
			double targetEdgeVmCapacity = (double)100 - edgeVMList.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());

			if (wanBW < 6 && requiredEdgeVmCapacity <= targetEdgeVmCapacity) {
				result = SimSettings.GENERIC_EDGE_DEVICE_ID;
			} else {
				List<MobileVM> mobileVMList = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
				double requiredMobileVmCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(mobileVMList.get(0).getVmType());
				double targetMobileVmCapacity = (double)100 - mobileVMList.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());

				if (requiredMobileVmCapacity <= targetMobileVmCapacity) {
					result = SimSettings.MOBILE_DATACENTER_ID;
				} else {
					result = SimSettings.CLOUD_DATACENTER_ID;

					// check
					wanDelay = SimManager.getInstance().getNetworkModel().getUploadDelay(
							task.getMobileDeviceId(), SimSettings.CLOUD_DATACENTER_ID, task);
					wanBW = (wanDelay == 0) ? 0 : (1 / wanDelay);

					if (wanBW <= 6 && Math.abs(wanBW - 6) - Math.abs(edgeUtilization - 70) >= 0) {
						result = SimSettings.MOBILE_DATACENTER_ID;
					}

				}
			}







			// Success: 6	70
//			List<MobileVM> vmArray = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
////			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(0).getVmType());
////			double targetVmCapacity = (double) 100 - vmArray.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
////
////
////			if (requiredCapacity <= targetVmCapacity) {
////				if (wanBW > 6 && edgeUtilization > 70) {
////					if ((wanBW - 6) - (edgeUtilization - 70) >= 0) {
////						result = SimSettings.MOBILE_DATACENTER_ID;
////					} else {
////						result = SimSettings.CLOUD_DATACENTER_ID;
////					}
////				} else {
////					if (Math.abs(wanBW - 6) - Math.abs(edgeUtilization - 70) >= 0) {
////						result = SimSettings.MOBILE_DATACENTER_ID;
////					} else {
////						result = SimSettings.GENERIC_EDGE_DEVICE_ID;
////					}
////				}
////			} else {
////				if (wanBW > 6 && edgeUtilization > 70) {
////					if ((wanBW - 6) - (edgeUtilization - 70) >= 0) {
////						result = SimSettings.GENERIC_EDGE_DEVICE_ID;
////					} else {
////						result = SimSettings.CLOUD_DATACENTER_ID;
////					}
////				} else {
////					if (Math.abs(wanBW - 6) - Math.abs(edgeUtilization - 70) >= 0) {
////						result = SimSettings.GENERIC_EDGE_DEVICE_ID;
////					} else {
////						result = SimSettings.CLOUD_DATACENTER_ID;
////					}
////				}
////			}




//			if (requiredCapacity > targetVmCapacity || wanBW <= 6 || edgeUtilization <= 60) {
//				result = SimSettings.GENERIC_EDGE_DEVICE_ID;
//			} else if (wanBW > 6 && edgeUtilization > 60) {
//				result = SimSettings.CLOUD_DATACENTER_ID;
//			} else {
//				result = SimSettings.MOBILE_DATACENTER_ID;
//			}



//			if (wanBW > 6 && edgeUtilization > 60) {
//				result = SimSettings.CLOUD_DATACENTER_ID;
//			} else {
//
//				List<MobileVM> vmArray = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
//				double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(0).getVmType());
//				double targetVmCapacity = (double) 100 - vmArray.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
//
//				if (requiredCapacity <= targetVmCapacity) {
//					result = SimSettings.MOBILE_DATACENTER_ID;
//				} else {
//					result = SimSettings.GENERIC_EDGE_DEVICE_ID;
//				}
//			}


//			if (requiredCapacity <= targetVmCapacity) {
//				result = SimSettings.MOBILE_DATACENTER_ID;
//			} else {
//				double utilization = edgeUtilization;
//				if (wanBW > 6 && utilization > 60) {
//					result = SimSettings.CLOUD_DATACENTER_ID;
//				} else {
//					result = SimSettings.GENERIC_EDGE_DEVICE_ID;
//				}
//			}

		} else {
			SimLogger.printLine("Unknow edge orchestrator policy! Terminating simulation...");
			System.exit(0);
		}

		return result;
	}

	/**
	 * It will be called by {@link CustomMobileDeviceManager#processOtherEvent(SimEvent)}
	 * to decide which VM will be chosen
	 */
	@Override
	public Vm getVmToOffload(Task task, int deviceId) {

		Vm selectedVM = null;

		if (deviceId == SimSettings.CLOUD_DATACENTER_ID) {
			//Select VM on cloud devices via Least Loaded algorithm!
			double selectedVmCapacity = 0; //start with min value
			List<Host> list = SimManager.getInstance().getCloudServerManager().getDatacenter().getHostList();
			for (int hostIndex = 0; hostIndex < list.size(); hostIndex++) {
				List<CloudVM> vmArray = SimManager.getInstance().getCloudServerManager().getVmList(hostIndex);
				for(int vmIndex=0; vmIndex < vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
	            }
			}
		} else if (deviceId == SimSettings.GENERIC_EDGE_DEVICE_ID) {
			//Select VM on edge devices via Least Loaded algorithm!
			double selectedVmCapacity = 0; //start with min value
			for(int hostIndex = 0; hostIndex < numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
				for(int vmIndex = 0; vmIndex < vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
						selectedVM = vmArray.get(vmIndex);
						selectedVmCapacity = targetVmCapacity;
					}
				}
			}
		} else if (deviceId == SimSettings.MOBILE_DATACENTER_ID) {
			List<MobileVM> mobileVMList = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(
					mobileVMList.get(0).getVmType());
			double targetVmCapacity = (double)100 - mobileVMList.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());

			if (requiredCapacity <= targetVmCapacity)
				selectedVM = mobileVMList.get(0);
		}
		else{
			SimLogger.printLine("Unknown device id! The simulation has been terminated.");
			System.exit(0);
		}
		
		return selectedVM;
	}

	@Override
	public void processEvent(SimEvent arg0) {
		// Nothing to do!
	}

	@Override
	public void shutdownEntity() {
		// Nothing to do!
	}

	@Override
	public void startEntity() {
		// Nothing to do!
	}

}
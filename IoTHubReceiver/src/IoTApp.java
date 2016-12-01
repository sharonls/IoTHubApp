import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.function.Consumer;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.iot.service.exceptions.IotHubException;
import com.microsoft.azure.iot.service.sdk.Device;
import com.microsoft.azure.iot.service.sdk.RegistryManager;
import com.microsoft.azure.servicebus.ServiceBusException;

public class IoTApp {

	private static final String connectionString = "HostName=RAIoTHubTest.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=+N9yM34EcVIhHJs7BWnYEKLCChdgzhauf2HKy7SfVeM=";
	private static final String deviceId = "device1";
	
	private static String connStr = "Endpoint=sb://ihsuprodbyres039dednamespace.servicebus.windows.net/;EntityPath=iothub-ehub-raiothubte-84293-ad77a7c8a2;SharedAccessKeyName=iothubowner;SharedAccessKey=+N9yM34EcVIhHJs7BWnYEKLCChdgzhauf2HKy7SfVeM=";
	
	public static void main(String[] args) throws IOException,
			URISyntaxException, Exception {
		RegistryManager registryManager = RegistryManager
				.createFromConnectionString(connectionString);

		Device device = Device.createFromId(deviceId, null, null);
		System.out.println(device.getStatus());
		try {
			device = registryManager.addDevice(device);
			System.out.println(device.getStatus());
		} catch (IotHubException iote) {
			try {
				device = registryManager.getDevice(deviceId);
			} catch (IotHubException iotf) {
				iotf.printStackTrace();
			}
		}
		System.out.println(device.getStatus());
		System.out.println("Device id: " + device.getDeviceId());
		System.out.println("Device key: " + device.getPrimaryKey());
		
		EventHubClient client0 = receiveMessages("0");
		EventHubClient client1 = receiveMessages("1");
		System.out.println("Press ENTER to exit.");
		System.in.read();
		try
		{
		  client0.closeSync();
		  client1.closeSync();
		  System.exit(0);
		}
		catch (ServiceBusException sbe)
		{
		  System.exit(1);
		}
	}
	
	 private static EventHubClient receiveMessages(final String partitionId)
	 {
	   EventHubClient client = null;
	   try {
	     client = EventHubClient.createFromConnectionStringSync(connStr);
	   }
	   catch(Exception e) {
	     System.out.println("Failed to create client: " + e.getMessage());
	     System.exit(1);
	   }
	   try {
	     client.createReceiver( 
	       EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,  
	       partitionId,  
	       Instant.now()).thenAccept(new Consumer<PartitionReceiver>()
	     {
	       public void accept(PartitionReceiver receiver)
	       {
	         System.out.println("** Created receiver on partition " + partitionId);
	         try {
	           while (true) {
	             Iterable<EventData> receivedEvents = receiver.receive(100).get();
	             int batchSize = 0;
	             if (receivedEvents != null)
	             {
	               for(EventData receivedEvent: receivedEvents)
	               {
	                 System.out.println(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s", 
	                   receivedEvent.getSystemProperties().getOffset(), 
	                   receivedEvent.getSystemProperties().getSequenceNumber(), 
	                   receivedEvent.getSystemProperties().getEnqueuedTime()));
	                 System.out.println(String.format("| Device ID: %s", receivedEvent.getProperties().get("iothub-connection-device-id")));
	                 System.out.println(String.format("| Message Payload: %s", new String(receivedEvent.getBody(),
	                   Charset.defaultCharset())));
	                 batchSize++;
	               }
	             }
	             System.out.println(String.format("Partition: %s, ReceivedBatch Size: %s", partitionId,batchSize));
	           }
	         }
	         catch (Exception e)
	         {
	           System.out.println("Failed to receive messages: " + e.getMessage());
	         }
	       }
	     });
	   }
	   catch (Exception e)
	   {
	     System.out.println("Failed to create receiver: " + e.getMessage());
	   }
	   return client;
	 }
	 
//	 private static class TelemetryDataPoint {
//		   public String deviceId;
//		   public double windSpeed;
//
//		   public String serialize() {
//		     Gson gson = new Gson();
//		     return gson.toJson(this);
//		   }
//	 }
//	 
//	 private static class EventCallback implements IotHubEventCallback
//	 {
//	   public void execute(IotHubStatusCode status, Object context) {
//	     System.out.println("IoT Hub responded to message with status: " + status.name());
//
//	     if (context != null) {
//	       synchronized (context) {
//	         context.notify();
//	       }
//	     }
//	   }
//	 }
//	 
//	 private static class MessageSender implements Runnable {
//		  public volatile boolean stopThread = false;
//
//		  public void run()  {
//		    try {
//		      double avgWindSpeed = 10; // m/s
//		      Random rand = new Random();
//
//		      while (!stopThread) {
//		        double currentWindSpeed = avgWindSpeed + rand.nextDouble() * 4 - 2;
//		        TelemetryDataPoint telemetryDataPoint = new TelemetryDataPoint();
//		        telemetryDataPoint.deviceId = deviceId;
//		        telemetryDataPoint.windSpeed = currentWindSpeed;
//
//		        String msgStr = telemetryDataPoint.serialize();
//		        Message msg = new Message(msgStr);
//		        System.out.println("Sending: " + msgStr);
//
//		        Object lockobj = new Object();
//		        EventCallback callback = new EventCallback();
//		        client.sendEventAsync(msg, callback, lockobj);
//
//		        synchronized (lockobj) {
//		          lockobj.wait();
//		        }
//		        Thread.sleep(1000);
//		      }
//		    } catch (InterruptedException e) {
//		      System.out.println("Finished.");
//		    }
//		  }
//		}

}

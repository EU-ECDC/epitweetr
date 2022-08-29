package org.ecdc.epitweetr

import sun.management.VMManagement;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import java.io.FileWriter; 
import java.io.IOException;  

object ProcessID {

	def PID = {
		val runtime = ManagementFactory.getRuntimeMXBean();
		val jvm = runtime.getClass().getDeclaredField("jvm");
		jvm.setAccessible(true);

		val management =  jvm.get(runtime).asInstanceOf[(VMManagement)];
		val method = management.getClass().getDeclaredMethod("getProcessId");
		method.setAccessible(true);
		method.invoke(management)
	}

  def writePID(path:String) = {
     val writer = new FileWriter(path)
     val pid = PID
     print(s"running with PID: ${pid}")
     writer.write(s"${pid.toString()}\n")
     writer.close()
  }
}

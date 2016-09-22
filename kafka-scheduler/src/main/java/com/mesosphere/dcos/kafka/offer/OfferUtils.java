package com.mesosphere.dcos.kafka.offer;

import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.TaskInfo;

import com.mesosphere.dcos.kafka.config.JmxConfig;

public class OfferUtils {
  public static String getConfigName(TaskInfo taskInfo) {
    for (Label label : taskInfo.getLabels().getLabelsList()) {
      if (label.getKey().equals("config_target")) {
        return label.getValue();
      }
    }

    return null;
  }

  public static String idToName(Integer brokerId) {
    return "broker-" + Integer.toString(brokerId);
  }

  public static int nameToId(String brokerName) {
    return Integer.parseInt(brokerName.substring(brokerName.indexOf('-') + 1));
  }
  
  public static String getKafkaJmxOpts(JmxConfig heapConfig) {
      
      if(heapConfig == null) {
          return "";
      }
      
      StringBuilder sb = new StringBuilder();
      
      if(!heapConfig.isRemote()) {
          sb.append("-Dcom.sun.management.jmxremote=");
          sb.append(heapConfig.isRemote());
          sb.append(" ");
      }
      
      if(heapConfig.getRemotePort() > 0) {
          sb.append("-Dcom.sun.management.jmxremote.port=");
          sb.append(heapConfig.getRemotePort());
          sb.append(" ");
      }
      
      if(heapConfig.isRemoteRegistrySsl()) {
          sb.append("-Dcom.sun.management.jmxremote.registry.ssl=");
          sb.append(heapConfig.isRemoteRegistrySsl());
          sb.append(" ");
      }
      
      if(!heapConfig.isRemoteSsl()) {
          sb.append("-Dcom.sun.management.jmxremote.ssl=");
          sb.append(heapConfig.isRemoteSsl());
          sb.append(" ");
      }
      
      if(heapConfig.getRemoteSslEnabledProtocols() != null) {
          sb.append("-Dcom.sun.management.jmxremote.ssl.enabled.protocols=");
          sb.append(heapConfig.getRemoteSslEnabledProtocols());
          sb.append(" ");
      }
      
      if(heapConfig.getRemoteSslEnabledCipherSuites() != null) {
          sb.append("-Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=");
          sb.append(heapConfig.getRemoteSslEnabledCipherSuites());
          sb.append(" ");
      }
      
      if(!heapConfig.isRemoteSslNeedClientAuth()) {
          sb.append("-Dcom.sun.management.jmxremote.ssl.need.client.auth=");
          sb.append(heapConfig.isRemoteSslNeedClientAuth());
          sb.append(" ");
      }
     
      if(!heapConfig.isRemoteAuthenticate()) {
         sb.append("-Dcom.sun.management.jmxremote.authenticate=");
         sb.append(heapConfig.isRemoteAuthenticate());
         sb.append(" ");
      }
      
      if(heapConfig.getRemotePasswordFile() != null) {
          sb.append("-Dcom.sun.management.jmxremote.password.file=");
          sb.append(heapConfig.getRemotePasswordFile());
          sb.append(" ");
      }
      
      if(heapConfig.getRemoteAccessFile() != null) {
          sb.append("-Dcom.sun.management.jmxremote.access.file=");
          sb.append(heapConfig.getRemoteAccessFile());
          sb.append(" ");
      }
      
      if(heapConfig.getRemoteLoginConfig() != null) {
          sb.append("-Dcom.sun.management.jmxremote.login.config=");
          sb.append(heapConfig.getRemoteLoginConfig());
          sb.append(" ");
      }
      
      //remove trailing whitespace (if any)
      if (sb.length() > 0 && sb.charAt(sb.length() -1) == ' ') {
          sb.setLength(sb.length() - 1);
      }
      return sb.toString();
  }
}

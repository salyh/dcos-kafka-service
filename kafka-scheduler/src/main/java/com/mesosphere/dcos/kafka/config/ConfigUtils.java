package com.mesosphere.dcos.kafka.config;

public class ConfigUtils {

    public static String getKafkaJmxOpts(JmxConfig jmxConfig) {
          
          if(jmxConfig == null) {
              return "";
          }
          
          StringBuilder sb = new StringBuilder();
          
          if(!jmxConfig.isRemote()) {
              sb.append("-Dcom.sun.management.jmxremote=");
              sb.append(jmxConfig.isRemote());
              sb.append(" ");
          }
          
          if(jmxConfig.getRemotePort() > 0) {
              sb.append("-Dcom.sun.management.jmxremote.port=");
              sb.append(jmxConfig.getRemotePort());
              sb.append(" ");
          }
          
          if(jmxConfig.isRemoteRegistrySsl()) {
              sb.append("-Dcom.sun.management.jmxremote.registry.ssl=");
              sb.append(jmxConfig.isRemoteRegistrySsl());
              sb.append(" ");
          }
          
          if(!jmxConfig.isRemoteSsl()) {
              sb.append("-Dcom.sun.management.jmxremote.ssl=");
              sb.append(jmxConfig.isRemoteSsl());
              sb.append(" ");
          }
          
          if(jmxConfig.getRemoteSslEnabledProtocols() != null) {
              sb.append("-Dcom.sun.management.jmxremote.ssl.enabled.protocols=");
              sb.append(jmxConfig.getRemoteSslEnabledProtocols());
              sb.append(" ");
          }
          
          if(jmxConfig.getRemoteSslEnabledCipherSuites() != null) {
              sb.append("-Dcom.sun.management.jmxremote.ssl.enabled.cipher.suites=");
              sb.append(jmxConfig.getRemoteSslEnabledCipherSuites());
              sb.append(" ");
          }
          
          if(!jmxConfig.isRemoteSslNeedClientAuth()) {
              sb.append("-Dcom.sun.management.jmxremote.ssl.need.client.auth=");
              sb.append(jmxConfig.isRemoteSslNeedClientAuth());
              sb.append(" ");
          }
         
          if(!jmxConfig.isRemoteAuthenticate()) {
             sb.append("-Dcom.sun.management.jmxremote.authenticate=");
             sb.append(jmxConfig.isRemoteAuthenticate());
             sb.append(" ");
          }
          
          if(jmxConfig.getRemotePasswordFile() != null) {
              sb.append("-Dcom.sun.management.jmxremote.password.file=");
              sb.append(jmxConfig.getRemotePasswordFile());
              sb.append(" ");
          }
          
          if(jmxConfig.getRemoteAccessFile() != null) {
              sb.append("-Dcom.sun.management.jmxremote.access.file=");
              sb.append(jmxConfig.getRemoteAccessFile());
              sb.append(" ");
          }
          
          if(jmxConfig.getRemoteLoginConfig() != null) {
              sb.append("-Dcom.sun.management.jmxremote.login.config=");
              sb.append(jmxConfig.getRemoteLoginConfig());
              sb.append(" ");
          }
          
          //remove trailing whitespace (if any)
          if (sb.length() > 0 && sb.charAt(sb.length() -1) == ' ') {
              sb.setLength(sb.length() - 1);
          }
          return sb.toString();
      }

}

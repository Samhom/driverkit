package io.github.samhom.driverkit.rocketmq;

import io.github.samhom.driverkit.core.*;
import org.apache.rocketmq.client.exception.*;
import org.apache.rocketmq.remoting.protocol.ResponseCode;

final class RemotingFailures {
  private RemotingFailures() {}

  static ClientException.Kind classify(Throwable error, Operation operation) {
    error = FailureClassifier.unwrap(error);
    int code =
        error instanceof MQBrokerException
            ? ((MQBrokerException) error).getResponseCode()
            : error instanceof MQClientException
                ? ((MQClientException) error).getResponseCode()
                : -1;
    switch (code) {
      case ResponseCode.NO_PERMISSION:
        return ClientException.Kind.AUTHORIZATION;
      case 3001:
        return ClientException.Kind.AUTHENTICATION; // Remoting 5 USER_NOT_EXIST.
      case ResponseCode.MESSAGE_ILLEGAL:
      case ResponseCode.INVALID_PARAMETER:
        return ClientException.Kind.INVALID_REQUEST;
      case ResponseCode.TOPIC_NOT_EXIST:
        return ClientException.Kind.NOT_FOUND;
      case ResponseCode.FLOW_CONTROL:
        return ClientException.Kind.RATE_LIMITED;
      case ResponseCode.SYSTEM_BUSY:
        return ClientException.Kind.OVERLOADED;
      case ResponseCode.FLUSH_DISK_TIMEOUT:
      case ResponseCode.FLUSH_SLAVE_TIMEOUT:
      case ResponseCode.SLAVE_NOT_AVAILABLE:
        return FailureClassifier.uncertain(operation, ClientException.Kind.TIMEOUT);
      default:
        break;
    }
    ClientException.Kind common = FailureClassifier.transport(error, operation);
    if (common != ClientException.Kind.DRIVER_FAILURE) return common;
    if (error instanceof org.apache.rocketmq.remoting.exception.RemotingTimeoutException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.TIMEOUT);
    if (error instanceof org.apache.rocketmq.remoting.exception.RemotingConnectException)
      return ClientException.Kind.CONNECTION_FAILURE;
    if (error instanceof MQBrokerException
        || error instanceof MQClientException
        || error instanceof org.apache.rocketmq.remoting.exception.RemotingException)
      return FailureClassifier.uncertain(operation, ClientException.Kind.SERVER_REJECTED);
    return ClientException.Kind.DRIVER_FAILURE;
  }
}

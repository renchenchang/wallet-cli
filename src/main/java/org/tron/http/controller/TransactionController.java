package org.tron.http.controller;

import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.tron.common.utils.ByteArray;
import org.tron.walletserver.WalletApi;


@RestController
public class TransactionController {

  @PostMapping("/broadcast")
  public boolean transactionFromView(String hexTransaction) throws InvalidProtocolBufferException {
    if (hexTransaction.isEmpty()) {
      return false;
    }
    byte[] bytes = ByteArray.fromHexString(hexTransaction);
    return WalletApi.broadcastTransaction(bytes);
  }
}
package org.tron.tool;

import com.google.protobuf.ByteString;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Random;
import org.tron.api.GrpcAPI.Return;
import org.tron.api.GrpcAPI.TransactionExtention;
import org.tron.api.GrpcAPI.TransactionSignWeight;
import org.tron.api.GrpcAPI.TransactionSignWeight.Result.response_code;
import org.tron.common.crypto.ECKey;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.TransactionUtils;
import org.tron.core.config.Configuration;
import org.tron.core.config.Parameter.CommonConstant;
import org.tron.core.exception.CancelException;
import org.tron.core.exception.CipherException;
import org.tron.protos.Contract;
import org.tron.protos.Protocol.Transaction;
import org.tron.walletserver.GrpcClient;
import org.tron.walletserver.WalletApi;

public class Exchange {

  private static GrpcClient rpcCli = init();

  public static GrpcClient init() {
    Config config = Configuration.getByPath("config.conf");

    String fullNode = "";
    String solidityNode = "";
    if (config.hasPath("soliditynode.ip.list")) {
      solidityNode = config.getStringList("soliditynode.ip.list").get(0);
    }
    if (config.hasPath("fullnode.ip.list")) {
      fullNode = config.getStringList("fullnode.ip.list").get(0);
    }
    if (config.hasPath("net.type") && "mainnet".equalsIgnoreCase(config.getString("net.type"))) {
      WalletApi.setAddressPreFixByte(CommonConstant.ADD_PRE_FIX_BYTE_MAINNET);
    } else {
      WalletApi.setAddressPreFixByte(CommonConstant.ADD_PRE_FIX_BYTE_TESTNET);
    }
    return new GrpcClient(fullNode, solidityNode);
  }

  public static boolean myProcessTransactionExtention(TransactionExtention transactionExtention,
      String privateKey)
      throws CancelException {
    if (transactionExtention == null) {
      return false;
    }
    Return ret = transactionExtention.getResult();
    if (!ret.getResult()) {
      System.out.println("Code = " + ret.getCode());
      System.out.println("Message = " + ret.getMessage().toStringUtf8());
      return false;
    }
    Transaction transaction = transactionExtention.getTransaction();
    if (transaction == null || transaction.getRawData().getContractCount() == 0) {
      System.out.println("Transaction is empty");
      return false;
    }
    System.out.println(
        "Receive txid = " + ByteArray.toHexString(transactionExtention.getTxid().toByteArray()));
    transaction = signTransaction(transaction, privateKey);
    return rpcCli.broadcastTransaction(transaction);
  }

  public static TransactionSignWeight getTransactionSignWeight(Transaction transaction) {
    return rpcCli.getTransactionSignWeight(transaction);
  }

  private static Transaction signTransaction(Transaction transaction, String privateKey) {
    transaction = TransactionUtils.sign(transaction, ECKey.fromPrivate(ByteArray.fromHexString(privateKey)));
    return transaction;
  }

  public static boolean exchangeTransaction(long exchangeId, byte[] tokenId, long quant,
      long expected, String privateKey)
      throws CancelException {
    ECKey ecKey = ECKey.fromPrivate(ByteArray.fromHexString(privateKey));
    byte[] from = ecKey.getAddress();
    Contract.ExchangeTransactionContract contract = createExchangeTransactionContract(from,
        exchangeId, tokenId, quant, expected);
    TransactionExtention transactionExtention = rpcCli.exchangeTransaction(contract);
    return myProcessTransactionExtention(transactionExtention, privateKey);
  }

  public static Contract.ExchangeTransactionContract createExchangeTransactionContract(byte[] owner,
      long exchangeId, byte[] tokenId, long quant, long expected) {
    Contract.ExchangeTransactionContract.Builder builder = Contract.ExchangeTransactionContract
        .newBuilder();
    builder
        .setOwnerAddress(ByteString.copyFrom(owner))
        .setExchangeId(exchangeId)
        .setTokenId(ByteString.copyFrom(tokenId))
        .setQuant(quant)
        .setExpected(expected);
    return builder.build();
  }


  public static void main(String[] args)
      throws CancelException, InterruptedException {
    Random r = new Random(100);
    for (int i = 0; i < 15; i++) {
      int num1 = r.nextInt(30);
      int num2 = r.nextInt(1000);
      int num3 = r.nextInt(50);
      int num4 = r.nextInt(100000);
      exchangeTransaction(21111, "_".getBytes(), num1 * 1000000, num2,
          "");
      Thread.sleep(2000);
      exchangeTransaction(21111, "_".getBytes(), num3 * 1000000, num4,
          "");

      Thread.sleep(3000);
    }
    System.out.println("END");
  }
}

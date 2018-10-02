package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Longs;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.tron.common.crypto.Sha256Hash;
import org.tron.common.utils.ByteArray;
import org.tron.protos.Contract;
import org.tron.protos.Contract.AccountCreateContract;
import org.tron.protos.Contract.AccountPermissionUpdateContract;
import org.tron.protos.Contract.AccountUpdateContract;
import org.tron.protos.Contract.AssetIssueContract;
import org.tron.protos.Contract.CreateSmartContract;
import org.tron.protos.Contract.ExchangeCreateContract;
import org.tron.protos.Contract.ExchangeInjectContract;
import org.tron.protos.Contract.ExchangeTransactionContract;
import org.tron.protos.Contract.ExchangeWithdrawContract;
import org.tron.protos.Contract.FreezeBalanceContract;
import org.tron.protos.Contract.ParticipateAssetIssueContract;
import org.tron.protos.Contract.PermissionAddKeyContract;
import org.tron.protos.Contract.PermissionDeleteKeyContract;
import org.tron.protos.Contract.PermissionUpdateKeyContract;
import org.tron.protos.Contract.ProposalApproveContract;
import org.tron.protos.Contract.ProposalCreateContract;
import org.tron.protos.Contract.ProposalDeleteContract;
import org.tron.protos.Contract.SetAccountIdContract;
import org.tron.protos.Contract.TransferAssetContract;
import org.tron.protos.Contract.TransferContract;
import org.tron.protos.Contract.TriggerSmartContract;
import org.tron.protos.Contract.UnfreezeAssetContract;
import org.tron.protos.Contract.UnfreezeBalanceContract;
import org.tron.protos.Contract.UpdateAssetContract;
import org.tron.protos.Contract.UpdateSettingContract;
import org.tron.protos.Contract.VoteAssetContract;
import org.tron.protos.Contract.VoteWitnessContract;
import org.tron.protos.Contract.VoteWitnessContract.Vote;
import org.tron.protos.Contract.WithdrawBalanceContract;
import org.tron.protos.Contract.WitnessCreateContract;
import org.tron.protos.Contract.WitnessUpdateContract;
import org.tron.protos.Protocol.Block;
import org.tron.protos.Protocol.Exchange;
import org.tron.protos.Protocol.Key;
import org.tron.protos.Protocol.Permission;
import org.tron.protos.Protocol.Transaction;
import org.tron.walletserver.WalletApi;

public class Util {

  public static ArrayList<String> addressList = new ArrayList<>();
  public static EsImporter importer = new EsImporter();

  public static List<UpdateRequest> getUpdateBuilder(Block block, Transaction transaction, boolean full)
      throws IOException {
    List<UpdateRequest> list = new ArrayList<>();
    Transaction.Contract contract = transaction.getRawData().getContract(0);
    String owner = WalletApi.encode58Check(getOwner(contract));
   // ArrayList<String> to = getToAddress(contract);
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    try {
      switch (contract.getType()) {
        case UpdateAssetContract:
          UpdateAssetContract updateAssetContract = contract.getParameter()
              .unpack(UpdateAssetContract.class);
          UpdateRequest request = new UpdateRequest("asset_issue_contract", "asset_issue_contract",
             owner);
          JSONObject jsonObject = new JSONObject();
          jsonObject.put("description", updateAssetContract.getDescription().toStringUtf8());
          jsonObject.put("url", updateAssetContract.getUrl().toStringUtf8());
          request.doc(jsonObject.toJSONString(), "XContentType.JSON");
          list.add(request);
          break;
        case WitnessUpdateContract:
          WitnessUpdateContract witnessUpdateContract = contract.getParameter()
              .unpack(WitnessUpdateContract.class);
          request = new UpdateRequest("asset_issue_contract", "asset_issue_contract",
              owner);
          jsonObject = new JSONObject();
          jsonObject.put("url", witnessUpdateContract.getUpdateUrl().toStringUtf8());
          request.doc(jsonObject.toJSONString(), "XContentType.JSON");
          list.add(request);
          break;
        case ProposalApproveContract:
          ProposalApproveContract proposalApproveContract = contract.getParameter()
              .unpack(ProposalApproveContract.class);
          request = new UpdateRequest("proposals", "proposals", proposalApproveContract.getProposalId() + "");
          jsonObject = new JSONObject();
          if (proposalApproveContract.getIsAddApproval()) {
            String approved = importer
                .getProposalApprovedList(proposalApproveContract.getProposalId());
            if (approved.equals("")) {
              approved = owner;
            } else {
              approved = approved + ";" + owner;
            }
            jsonObject.put("approved", approved);
          }
          request.doc(jsonObject.toJSONString(), "XContentType.JSON");
          list.add(request);
          break;
        case ExchangeWithdrawContract:
        case ExchangeTransactionContract:
        case ExchangeInjectContract:
          ExchangeInjectContract exchangeInjectContract = contract.getParameter()
              .unpack(ExchangeInjectContract.class);
          request = new UpdateRequest("exchanges", "exchanges", exchangeInjectContract.getExchangeId() + "");
          jsonObject = new JSONObject();
          Optional<Exchange> exchangeOptional = WalletApi.getExchange(exchangeInjectContract.getExchangeId() + "");
          if (exchangeOptional.isPresent()) {
            Exchange exchange = exchangeOptional.get();
            jsonObject.put("first_token_id", exchange.getFirstTokenId().toStringUtf8());
            jsonObject.put("first_token_balance", exchange.getFirstTokenBalance());
            jsonObject.put("second_token_id", exchange.getSecondTokenId().toStringUtf8());
            jsonObject.put("second_token_balance", exchange.getSecondTokenBalance());
          }
          request.doc(jsonObject.toJSONString(), "XContentType.JSON");
          list.add(request);
          break;

        case UpdateSettingContract:
          UpdateSettingContract updateSettingContract = contract.getParameter()
              .unpack(UpdateSettingContract.class);
          request = new UpdateRequest("smart_contracts", "smart_contracts",
              WalletApi.encode58Check(updateSettingContract.getContractAddress().toByteArray()));
          jsonObject = new JSONObject();
          jsonObject.put("consume_user_resource_percent",
              updateSettingContract.getConsumeUserResourcePercent());
          request.doc(jsonObject.toJSONString(), "XContentType.JSON");
          list.add(request);
          break;

        default:
          return null;
      }
      return list;
    } catch (Exception e) {
      return null;
    }
  }

  public static List<IndexRequest> getIndexBuilder(Block block, Transaction transaction,
      boolean full) throws IOException {
    List<IndexRequest> list = new ArrayList<>();
    Transaction.Contract contract = transaction.getRawData().getContract(0);
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    String owner = WalletApi.encode58Check(getOwner(contract));
    ArrayList<String> to = getToAddress(contract);
    long createTime = transaction.getRawData().getTimestamp() == 0 ?
        block.getBlockHeader().getRawData().getTimestamp() : transaction.getRawData().getTimestamp();
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract = contract.getParameter()
              .unpack(TransferContract.class);
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", Util.getTxID(transaction));
          builder.field("owner_address",owner);
          builder.field("to_address", to);
          builder.field("confirmed", !full);
          builder.field("token_name", "trx");
          builder.field("amount", transferContract.getAmount());
          builder.endObject();
          IndexRequest indexRequest = new IndexRequest("transfers", "transfers",
              Util.getTxID(transaction))
              .source(builder);
          list.add(indexRequest);
          break;

        case AssetIssueContract:
          AssetIssueContract assetIssueContract = contract.getParameter()
              .unpack(AssetIssueContract.class);
          builder.field("owner_address", owner);
          builder.field("name", assetIssueContract.getName().toStringUtf8());
          builder.field("total_supply", assetIssueContract.getTotalSupply());
          builder.field("trx_num", assetIssueContract.getTrxNum());
          builder.field("num", assetIssueContract.getNum());
          builder.field("date_end", assetIssueContract.getEndTime());
          builder.field("date_start", assetIssueContract.getStartTime());
          builder.field("decay_ratio", "");
          builder.field("vote_score", assetIssueContract.getVoteScore());
          builder.field("description", assetIssueContract.getDescription().toStringUtf8());
          builder.field("url", assetIssueContract.getUrl().toStringUtf8());
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("frozen", "");
          builder.field("abbr", assetIssueContract.getAbbr().toStringUtf8());
          builder.endObject();
          indexRequest = new IndexRequest("asset_issue_contract", "asset_issue_contract",
              WalletApi.encode58Check(Util.getOwner(contract)))
              .source(builder);
          list.add(indexRequest);
          break;

        case ParticipateAssetIssueContract:
          ParticipateAssetIssueContract participateAssetIssueContract =
              contract.getParameter().unpack(ParticipateAssetIssueContract.class);
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("to_address", to);
          builder.field("token_name", participateAssetIssueContract.getAssetName().toStringUtf8());
          builder.field("amount", participateAssetIssueContract.getAmount());
          builder.endObject();
          indexRequest = new IndexRequest("participate_asset_issue", "participate_asset_issue",
              participateAssetIssueContract.getAssetName().toStringUtf8())
              .source(builder);
          list.add(indexRequest);
          break;

        case WitnessCreateContract:
          WitnessCreateContract witnessCreateContract =
              contract.getParameter().unpack(WitnessCreateContract.class);
          builder.field("address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("url", witnessCreateContract.getUrl().toStringUtf8());
          builder.endObject();
          indexRequest = new IndexRequest("witness_create_contract", "witness_create_contract",
              Util.getTxID(transaction))
              .source(builder);
          list.add(indexRequest);
          break;

        case VoteWitnessContract:
          VoteWitnessContract voteWitnessContract = contract.getParameter()
              .unpack(VoteWitnessContract.class);
          for (Vote vote : voteWitnessContract.getVotesList()) {
            String toAddress = WalletApi.encode58Check(vote.getVoteAddress().toByteArray());
            builder.field("block", block.getBlockHeader().getRawData().getNumber());
            builder.field("hash", getTxID(transaction));
            builder.field("date_created", createTime);
            builder.field("owner_address", owner);
            builder.field("candidate_address", toAddress);
            builder.field("vote_count", vote.getVoteCount());

            builder.endObject();
            indexRequest = new IndexRequest("vote_witness_contract", "vote_witness_contract",
                WalletApi.encode58Check(vote.getVoteAddress().toByteArray()))
                .source(builder);
            list.add(indexRequest);
          }
          break;

        case ProposalCreateContract:
          ProposalCreateContract proposalCreateContract =
              contract.getParameter().unpack(ProposalCreateContract.class);
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", createTime);
          builder.field("parameters", JsonFormat.printToString(proposalCreateContract));
          builder.field("approved", "");
          builder.field("id", (importer.getCurrentProposalID() + 1) + "");
          builder.field("confirmed", !full);
          builder.endObject();
          indexRequest = new IndexRequest("proposals", "proposals",
              (importer.getCurrentProposalID() + 1) + "")
              .source(builder);
          list.add(indexRequest);
          break;

        case ExchangeCreateContract:
          ExchangeCreateContract exchangeCreateContract =
              contract.getParameter().unpack(ExchangeCreateContract.class);
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", createTime);
          builder.field("first_token_id", exchangeCreateContract.getFirstTokenId().toStringUtf8());
          builder.field("first_token_balance", exchangeCreateContract.getFirstTokenBalance());
          builder.field("second_token_id", exchangeCreateContract.getSecondTokenId().toStringUtf8());
          builder.field("second_token_balance", exchangeCreateContract.getSecondTokenBalance());
          builder.field("id", (importer.getCurrentExchangeID() + 1) + "");
          builder.field("confirmed", !full);
          builder.endObject();
          indexRequest = new IndexRequest("exchanges", "exchanges",
              (importer.getCurrentExchangeID() + 1) + "")
              .source(builder);
          list.add(indexRequest);
          break;

        case ExchangeTransactionContract:
          ExchangeTransactionContract exchangeTransactionContract =
              contract.getParameter().unpack(ExchangeTransactionContract.class);
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", createTime);
          builder.field("exchange_id", exchangeTransactionContract.getExchangeId());
          builder.field("token_id", exchangeTransactionContract.getTokenId().toStringUtf8());
          builder.field("quant", exchangeTransactionContract.getQuant());
          builder.field("expected", exchangeTransactionContract.getExpected());
          builder.field("confirmed", !full);
          builder.endObject();
          indexRequest = new IndexRequest("exchange_transactions", "exchange_transactions",
              (importer.getCurrentExchangeID() + 1) + "")
              .source(builder);
          list.add(indexRequest);
          break;

        case CreateSmartContract:
          CreateSmartContract createSmartContract =
              contract.getParameter().unpack(CreateSmartContract.class);
          String contractAddress = WalletApi.encode58Check(
              createSmartContract.getNewContract().getContractAddress().toByteArray());
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", createTime);
          builder.field("smart_contract", JsonFormat.printToString(createSmartContract));
          builder.field("contract_address", contractAddress);
          builder.field("origin_address", WalletApi.encode58Check(
              createSmartContract.getNewContract().getOriginAddress().toByteArray()));
          builder.field("abi", JsonFormat.printToString(createSmartContract.getNewContract().getAbi()));
          builder.field("bytecode", ByteArray.toHexString(
              createSmartContract.getNewContract().getBytecode().toByteArray()));
          builder.field("call_value", createSmartContract.getNewContract().getCallValue());
          builder.field("consume_user_resource_percent", createSmartContract
          .getNewContract().getConsumeUserResourcePercent());
          builder.field("name", createSmartContract.getNewContract().getName());
          builder.field("confirmed", !full);
          builder.endObject();
          indexRequest = new IndexRequest("smart_contracts", "smart_contracts",
              contractAddress)
              .source(builder);
          list.add(indexRequest);
          break;

        case TriggerSmartContract:
          TriggerSmartContract triggerSmartContract =
              contract.getParameter().unpack(TriggerSmartContract.class);
          String contractCallAddress = WalletApi.encode58Check(
              triggerSmartContract.getContractAddress().toByteArray());
          builder.field("owner_address", owner);
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", createTime);
          builder.field("contract_address", contractCallAddress);
          builder.field("call_value", triggerSmartContract.getCallValue());
          builder.field("data", ByteArray.toHexString(triggerSmartContract.getData().toByteArray()));
          builder.field("confirmed", !full);
          builder.endObject();
          indexRequest = new IndexRequest("smart_contract_triggers", "smart_contract_triggers",
              contractCallAddress)
              .source(builder);
          list.add(indexRequest);
          break;

//        case FreezeBalanceContract:
//          FreezeBalanceContract freezeBalanceContract = contract.getParameter()
//              .unpack(FreezeBalanceContract.class);
//
//          builder.field("block", block.getBlockHeader().getRawData().getNumber());
//          builder.field("hash", getTxID(transaction));
//          builder.field("date_created", block.getBlockHeader().getRawData().getTimestamp());
//          builder.field("owner_address", WalletApi.encode58Check(Util.getOwner(contract)));
//          builder.field("frozen_balance", freezeBalanceContract.getFrozenBalance());
//          builder.field("frozen_duration", freezeBalanceContract.getFrozenDuration());
//          builder.field("type", "FreezeBalanceContract");
//          builder.endObject();
//          indexRequest = new IndexRequest("freeze_balance_contract", "freeze_balance_contract",
//              Util.getTxID(transaction))
//              .source(builder);
//          list.add(indexRequest);
//          break;
//
//        case UnfreezeBalanceContract:
//          UnfreezeBalanceContract unfreezeBalanceContract = contract.getParameter()
//              .unpack(UnfreezeBalanceContract.class);
//
//          builder.field("block", block.getBlockHeader().getRawData().getNumber());
//          builder.field("hash", getTxID(transaction));
//          builder.field("date_created", block.getBlockHeader().getRawData().getTimestamp());
//          builder.field("owner_address", WalletApi.encode58Check(Util.getOwner(contract)));
//          builder.field("type", "unfreezeBalanceContract");
//          builder.endObject();
//          indexRequest = new IndexRequest("freeze_balance_contract", "freeze_balance_contract",
//              Util.getTxID(transaction))
//              .source(builder);
//          list.add(indexRequest);
//          break;

        default:
          return null;
      }
      return list;
    } catch (Exception e) {
      return null;
    }
  }

  public static ArrayList<String> getToAddress(Transaction.Contract contract) {
    ArrayList<String> list = new ArrayList<>();
    for (ByteString bytes : getTo(contract)) {
      list.add(WalletApi.encode58Check(bytes.toByteArray()));
    }
    return null;
  }

  public static ArrayList<ByteString> getTo(Transaction.Contract contract) {
    ArrayList<ByteString> list = new ArrayList<>();
    try {
      Any contractParameter = contract.getParameter();
      switch (contract.getType()) {
        case AccountCreateContract:
          list.add(contractParameter.unpack(AccountCreateContract.class).getAccountAddress());
          break;
        case AccountUpdateContract:
          break;
        case SetAccountIdContract:
          break;
        case TransferContract:
          list.add(contractParameter.unpack(TransferContract.class).getToAddress());
          break;
        case TransferAssetContract:
          list.add(contractParameter.unpack(TransferAssetContract.class).getToAddress());
          break;
        case VoteAssetContract:
          list.addAll(contractParameter.unpack(VoteAssetContract.class).getVoteAddressList());
          break;
        case VoteWitnessContract:
          for (Vote vote : contractParameter.unpack(VoteWitnessContract.class).getVotesList()) {
            list.add(vote.getVoteAddress());
          }
          break;
        case WitnessCreateContract:
          break;
        case AssetIssueContract:
          break;
        case WitnessUpdateContract:
          break;
        case ParticipateAssetIssueContract:
          list.add(contractParameter.unpack(ParticipateAssetIssueContract.class).getToAddress());
          break;
        case FreezeBalanceContract:
          break;
        case UnfreezeBalanceContract:
          break;
        case UnfreezeAssetContract:
          break;
        case WithdrawBalanceContract:
          break;
        case CreateSmartContract:
          break;
        case TriggerSmartContract:
          break;
        case UpdateAssetContract:
          break;
        case ProposalCreateContract:
          break;
        case ProposalApproveContract:
          break;
        case ProposalDeleteContract:
          break;
//        case BuyStorageContract:
//          owner = contractParameter.unpack(BuyStorageContract.class).getOwnerAddress();
//          break;
//        case BuyStorageBytesContract:
//          owner = contractParameter.unpack(BuyStorageBytesContract.class).getOwnerAddress();
//          break;
//        case SellStorageContract:
//          owner = contractParameter.unpack(SellStorageContract.class).getOwnerAddress();
//          break;
        case UpdateSettingContract:
          break;
        case ExchangeCreateContract:
          break;
        case ExchangeInjectContract:
          break;
        case ExchangeWithdrawContract:
          break;
        case ExchangeTransactionContract:
          break;
        case AccountPermissionUpdateContract:
          for (Permission permission : contractParameter
              .unpack(AccountPermissionUpdateContract.class).getPermissionsList()) {
            for (Key key : permission.getKeysList()) {
              list.add(key.getAddress());
            }
          }
          break;
        case PermissionAddKeyContract:
          list.add(contractParameter.unpack(PermissionAddKeyContract.class).getKey().getAddress());
          break;
        case PermissionUpdateKeyContract:
          list.add(contractParameter.unpack(PermissionUpdateKeyContract.class).getKey().getAddress());
          break;
        case PermissionDeleteKeyContract:
          list.add(contractParameter.unpack(PermissionDeleteKeyContract.class).getKeyAddress());
          break;
        // todo add other contract
        default:
          return null;
      }
      return list;
    } catch (Exception ex) {
      return null;
    }
  }

  public static byte[] getOwner(Transaction.Contract contract) {
    ByteString owner;
    try {
      Any contractParameter = contract.getParameter();
      switch (contract.getType()) {
        case AccountCreateContract:
          owner = contractParameter.unpack(AccountCreateContract.class).getOwnerAddress();
          break;
        case AccountUpdateContract:
          owner = contractParameter.unpack(AccountUpdateContract.class).getOwnerAddress();
          break;
        case SetAccountIdContract:
          owner = contractParameter.unpack(SetAccountIdContract.class).getOwnerAddress();
          break;
        case TransferContract:
          owner = contractParameter.unpack(TransferContract.class).getOwnerAddress();
          break;
        case TransferAssetContract:
          owner = contractParameter.unpack(TransferAssetContract.class).getOwnerAddress();
          break;
        case VoteAssetContract:
          owner = contractParameter.unpack(VoteAssetContract.class).getOwnerAddress();
          break;
        case VoteWitnessContract:
          owner = contractParameter.unpack(VoteWitnessContract.class).getOwnerAddress();
          break;
        case WitnessCreateContract:
          owner = contractParameter.unpack(WitnessCreateContract.class).getOwnerAddress();
          break;
        case AssetIssueContract:
          owner = contractParameter.unpack(AssetIssueContract.class).getOwnerAddress();
          break;
        case WitnessUpdateContract:
          owner = contractParameter.unpack(WitnessUpdateContract.class).getOwnerAddress();
          break;
        case ParticipateAssetIssueContract:
          owner = contractParameter.unpack(ParticipateAssetIssueContract.class).getOwnerAddress();
          break;
        case FreezeBalanceContract:
          owner = contractParameter.unpack(FreezeBalanceContract.class).getOwnerAddress();
          break;
        case UnfreezeBalanceContract:
          owner = contractParameter.unpack(UnfreezeBalanceContract.class).getOwnerAddress();
          break;
        case UnfreezeAssetContract:
          owner = contractParameter.unpack(UnfreezeAssetContract.class).getOwnerAddress();
          break;
        case WithdrawBalanceContract:
          owner = contractParameter.unpack(WithdrawBalanceContract.class).getOwnerAddress();
          break;
        case CreateSmartContract:
          owner = contractParameter.unpack(Contract.CreateSmartContract.class).getOwnerAddress();
          break;
        case TriggerSmartContract:
          owner = contractParameter.unpack(Contract.TriggerSmartContract.class).getOwnerAddress();
          break;
        case UpdateAssetContract:
          owner = contractParameter.unpack(UpdateAssetContract.class).getOwnerAddress();
          break;
        case ProposalCreateContract:
          owner = contractParameter.unpack(ProposalCreateContract.class).getOwnerAddress();
          break;
        case ProposalApproveContract:
          owner = contractParameter.unpack(ProposalApproveContract.class).getOwnerAddress();
          break;
        case ProposalDeleteContract:
          owner = contractParameter.unpack(ProposalDeleteContract.class).getOwnerAddress();
          break;
//        case BuyStorageContract:
//          owner = contractParameter.unpack(BuyStorageContract.class).getOwnerAddress();
//          break;
//        case BuyStorageBytesContract:
//          owner = contractParameter.unpack(BuyStorageBytesContract.class).getOwnerAddress();
//          break;
//        case SellStorageContract:
//          owner = contractParameter.unpack(SellStorageContract.class).getOwnerAddress();
//          break;
        case UpdateSettingContract:
          owner = contractParameter.unpack(UpdateSettingContract.class)
              .getOwnerAddress();
          break;
        case ExchangeCreateContract:
          owner = contractParameter.unpack(ExchangeCreateContract.class).getOwnerAddress();
          break;
        case ExchangeInjectContract:
          owner = contractParameter.unpack(ExchangeInjectContract.class).getOwnerAddress();
          break;
        case ExchangeWithdrawContract:
          owner = contractParameter.unpack(ExchangeWithdrawContract.class).getOwnerAddress();
          break;
        case ExchangeTransactionContract:
          owner = contractParameter.unpack(ExchangeTransactionContract.class).getOwnerAddress();
          break;
        case AccountPermissionUpdateContract:
          owner = contractParameter.unpack(AccountPermissionUpdateContract.class).getOwnerAddress();
          break;
        case PermissionAddKeyContract:
          owner = contractParameter.unpack(PermissionAddKeyContract.class).getOwnerAddress();
          break;
        case PermissionUpdateKeyContract:
          owner = contractParameter.unpack(PermissionUpdateKeyContract.class).getOwnerAddress();
          break;
        case PermissionDeleteKeyContract:
          owner = contractParameter.unpack(PermissionDeleteKeyContract.class).getOwnerAddress();
          break;
        // todo add other contract
        default:
          return null;
      }
      return owner.toByteArray();
    } catch (Exception ex) {
      return null;
    }
  }

  public static String getTxID(Transaction transaction) {
    return ByteArray.toHexString(Sha256Hash.hash(transaction.getRawData().toByteArray()));
  }

  public static String getBlockID(Block block) {
    long blockNum = block.getBlockHeader().getRawData().getNumber();
    byte[] blockHash = Sha256Hash.of(block.getBlockHeader().getRawData().toByteArray())
        .getByteString().toByteArray();
    byte[] numBytes = Longs.toByteArray(blockNum);
    byte[] hash = new byte[blockHash.length];
    System.arraycopy(numBytes, 0, hash, 0, 8);
    System.arraycopy(blockHash, 8, hash, 8, blockHash.length - 8);
    return ByteArray.toHexString(hash);
  }
}

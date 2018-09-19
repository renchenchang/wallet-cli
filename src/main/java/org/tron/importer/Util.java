package org.tron.importer;

import com.alibaba.fastjson.JSONObject;
import com.google.common.primitives.Longs;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.tron.protos.Protocol.Transaction;
import org.tron.walletserver.WalletApi;

public class Util {

  public static byte[] getTo(Transaction.Contract contract) {
    ByteString to;
    try {
      Any contractParameter = contract.getParameter();
      switch (contract.getType()) {
        case AccountCreateContract:
          to = contractParameter.unpack(AccountCreateContract.class).getOwnerAddress();
          break;
        case TransferContract:
          to = contractParameter.unpack(TransferContract.class).getOwnerAddress();
          break;
        case TransferAssetContract:
          to = contractParameter.unpack(TransferAssetContract.class).getOwnerAddress();
          break;
        case VoteWitnessContract:
          to = contractParameter.unpack(VoteWitnessContract.class).getOwnerAddress();
          break;
        default:
          return null;
      }
      return to.toByteArray();
    } catch (Exception e) {
      return null;
    }
  }

  public static List<UpdateRequest> getUpdateBuilder(Block block, Transaction transaction, boolean full)
      throws IOException {
    List<UpdateRequest> list = new ArrayList<>();
    Transaction.Contract contract = transaction.getRawData().getContract(0);
    XContentBuilder builder = XContentFactory.jsonBuilder();
    builder.startObject();
    try {
      switch (contract.getType()) {
        case UpdateAssetContract:
          UpdateAssetContract updateAssetContract = contract.getParameter()
              .unpack(UpdateAssetContract.class);
          UpdateRequest request = new UpdateRequest("asset_issue_contract", "asset_issue_contract",
              WalletApi.encode58Check(Util.getOwner(contract)));
          JSONObject jsonObject = new JSONObject();
          jsonObject.put("description", updateAssetContract.getDescription().toStringUtf8());
          jsonObject.put("url", updateAssetContract.getUrl().toStringUtf8());
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
    try {
      switch (contract.getType()) {
        case TransferContract:
          TransferContract transferContract = contract.getParameter()
              .unpack(TransferContract.class);
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", Util.getTxID(transaction));
          builder.field("owner_address",
              WalletApi.encode58Check(Util.getOwner(contract)));
          builder.field("to_address", Util.getTo(contract));
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
          builder.field("owner_address",
              WalletApi.encode58Check(Util.getOwner(contract)));
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
          builder.field("owner_address",
              WalletApi.encode58Check(Util.getOwner(contract)));
          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", transaction.getRawData().getTimestamp());
          builder.field("to_address", WalletApi.encode58Check(
              participateAssetIssueContract.getToAddress().toByteArray()));
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
          builder.field("address",
              WalletApi.encode58Check(Util.getOwner(contract)));
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
            builder.field("block", block.getBlockHeader().getRawData().getNumber());
            builder.field("hash", getTxID(transaction));
            builder.field("date_created", transaction.getRawData().getTimestamp());
            builder.field("owner_address",
                WalletApi.encode58Check(Util.getOwner(contract)));

            builder.endObject();
            indexRequest = new IndexRequest("vote_witness_contract", "vote_witness_contract",
                WalletApi.encode58Check(vote.getVoteAddress().toByteArray()))
                .source(builder);
            list.add(indexRequest);
          }
          break;

        case FreezeBalanceContract:
          FreezeBalanceContract freezeBalanceContract = contract.getParameter()
              .unpack(FreezeBalanceContract.class);

          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", block.getBlockHeader().getRawData().getTimestamp());
          builder.field("owner_address", WalletApi.encode58Check(Util.getOwner(contract)));
          builder.field("frozen_balance", freezeBalanceContract.getFrozenBalance());
          builder.field("frozen_duration", freezeBalanceContract.getFrozenDuration());
          builder.field("type", "FreezeBalanceContract");
          builder.endObject();
          indexRequest = new IndexRequest("freeze_balance_contract", "freeze_balance_contract",
              Util.getTxID(transaction))
              .source(builder);
          list.add(indexRequest);
          break;

        case UnfreezeBalanceContract:
          UnfreezeBalanceContract unfreezeBalanceContract = contract.getParameter()
              .unpack(UnfreezeBalanceContract.class);

          builder.field("block", block.getBlockHeader().getRawData().getNumber());
          builder.field("hash", getTxID(transaction));
          builder.field("date_created", block.getBlockHeader().getRawData().getTimestamp());
          builder.field("owner_address", WalletApi.encode58Check(Util.getOwner(contract)));
          builder.field("type", "WithdrawBalanceContract");
          builder.endObject();
          indexRequest = new IndexRequest("freeze_balance_contract", "freeze_balance_contract",
              Util.getTxID(transaction))
              .source(builder);
          list.add(indexRequest);
          break;

        default:
          return null;
      }
      return list;
    } catch (Exception e) {
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
        case AccountUpdateContract:
          owner = contractParameter.unpack(AccountUpdateContract.class).getOwnerAddress();
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
        case SetAccountIdContract:
          owner = contractParameter.unpack(SetAccountIdContract.class).getOwnerAddress();
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

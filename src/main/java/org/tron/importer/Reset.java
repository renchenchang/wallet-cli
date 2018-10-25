package org.tron.importer;

import java.io.IOException;
import org.tron.walletserver.WalletApi;

public class Reset {

  public static void main(String[] args) throws IOException {
    EsImporter esImporter = new EsImporter();
    esImporter.deleteIndex("transaction_info");
    esImporter.deleteBlocksFrom(3500000);
  //  esImporter.parseBlock(WalletApi.getBlock4Loader(3112940, false), false);
  }
}

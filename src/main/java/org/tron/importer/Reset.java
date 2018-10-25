package org.tron.importer;

import java.io.IOException;
import org.tron.walletserver.WalletApi;

public class Reset {

  public static void main(String[] args) throws IOException {
    EsImporter esImporter = new EsImporter();
    esImporter.deleteIndex("statistics");
   // esImporter.deleteBlocksFrom(3112940);
  //  esImporter.parseBlock(WalletApi.getBlock4Loader(3112940, false), false);
  }
}

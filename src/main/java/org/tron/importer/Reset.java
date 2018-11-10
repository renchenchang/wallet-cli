package org.tron.importer;

import java.io.IOException;
import org.tron.walletserver.WalletApi;

public class Reset {

  public static void main(String[] args) throws IOException {
    EsImporter esImporter = new EsImporter();
    esImporter.deleteBlocksFrom(3908763);
 //   esImporter.resetDB();
//    esImporter.deleteIndex("exchanges");
//    esImporter.deleteIndex("exchange_prices");
 //   esImporter.deleteIndex("statistics");deleteIndex("exchanges");
 //   esImporter.deleteBlocksFrom(3500000);
  //  esImporter.parseBlock(WalletApi.getBlock4Loader(3112940, false), false);
  }
}

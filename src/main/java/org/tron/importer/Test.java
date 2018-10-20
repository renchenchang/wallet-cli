package org.tron.importer;

import java.io.IOException;
import org.tron.protos.Protocol.Block;
import org.tron.walletserver.WalletApi;

public class Test {

  public static void main(String[] args) throws IOException {
//    EsImporter esImporter = new EsImporter();
//    Block block = WalletApi.getBlock4Loader(	3347500, false);
//    esImporter.parseBlock(block, false);
    System.out.println(WalletApi.hours);
  }
}

package org.tron.http.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.tron.importer.Http;


@RestController
public class AccountController {

  @GetMapping("/api/account")
  public String getAccount(String address) {
    String url = "http://18.223.114.116:9200/accounts/accounts/_search?q=address:" + address;
    String result = Http.doGet(url, "utf-8", 5);

    return result;
  }

  public static void main(String[] args) {
    AccountController o = new AccountController();
    System.out.println(o.getAccount("TY7s1dhNJSDmbGDxqRmNATQAr9iNpYv6TZ"));
  }
}

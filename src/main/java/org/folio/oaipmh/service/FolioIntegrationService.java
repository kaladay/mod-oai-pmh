package org.folio.oaipmh.service;

import org.folio.oaipmh.Request;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;

public interface FolioIntegrationService {

  HttpClientRequest buildGetUpdatedInstanceIdsQuery(HttpClient httpClient, Request request );

  //todo
//  HttpClientRequest buildEnrichQuery(HttpClient httpClient, Request request );

}

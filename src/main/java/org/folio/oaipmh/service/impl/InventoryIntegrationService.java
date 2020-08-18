package org.folio.oaipmh.service.impl;

import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.service.Utils.convertStringToDate;
import static org.folio.oaipmh.service.Utils.shouldSkipSuppressedRecords;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.folio.oaipmh.Constants;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.RepositoryConfigurationUtil;
import org.folio.oaipmh.service.FolioIntegrationService;
import org.folio.rest.tools.utils.TenantTool;
import org.springframework.stereotype.Component;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Component
public class InventoryIntegrationService implements FolioIntegrationService {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private static final String DELETED_RECORD_SUPPORT_PARAM_NAME = "deletedRecordSupport";
  private static final String START_DATE_PARAM_NAME = "startDate";
  private static final String END_DATE_PARAM_NAME = "endDate";
  private static final String SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS = "skipSuppressedFromDiscoveryRecords";
  private static final String INVENTORY_UPDATED_INSTANCES_ENDPOINT = "/oai-pmh-view/updatedInstanceIds";

  public HttpClientRequest buildGetUpdatedInstanceIdsQuery(HttpClient httpClient, Request request) {
    Map<String, String> paramMap = new HashMap<>();
    Date date = convertStringToDate(request.getFrom(), false);
    if (date != null) {
      paramMap.put(START_DATE_PARAM_NAME, Constants.ISO_DATE_FORMAT.format(date));
    }
    date = convertStringToDate(request.getUntil(), true);
    if (date != null) {
      paramMap.put(END_DATE_PARAM_NAME, Constants.ISO_DATE_FORMAT.format(date));
    }
    paramMap.put(DELETED_RECORD_SUPPORT_PARAM_NAME,
      String.valueOf(
        RepositoryConfigurationUtil.isDeletedRecordsEnabled(request)));
    paramMap.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS,
      String.valueOf(
        shouldSkipSuppressedRecords(request)));

    final String params = paramMap.entrySet().stream()
      .map(e -> e.getKey() + "=" + e.getValue())
      .collect(Collectors.joining("&"));

    String inventoryQuery = String.format("%s%s?%s", request.getOkapiUrl(), INVENTORY_UPDATED_INSTANCES_ENDPOINT, params);


    logger.info("Sending request to :" + inventoryQuery);
    final HttpClientRequest httpClientRequest = httpClient
      .getAbs(inventoryQuery);

    httpClientRequest.putHeader(OKAPI_TOKEN, request.getOkapiToken());
    httpClientRequest.putHeader(OKAPI_TENANT, TenantTool.tenantId(request.getOkapiHeaders()));
    httpClientRequest.putHeader(ACCEPT, APPLICATION_JSON);

    return httpClientRequest;
  }
}

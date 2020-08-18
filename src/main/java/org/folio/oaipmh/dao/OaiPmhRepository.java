package org.folio.oaipmh.dao;

import java.util.List;

import org.folio.oaipmh.Request;
import org.folio.rest.persist.PostgresClient;

import io.vertx.core.Promise;
import io.vertx.core.parsetools.JsonEvent;

public interface OaiPmhRepository {
  Promise<Void> saveInstancesIds(List<JsonEvent> instances, Request request, String requestId);
}

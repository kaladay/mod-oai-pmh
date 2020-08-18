package org.folio.oaipmh.helpers.streaming;

import java.util.List;

import org.folio.oaipmh.Request;

import io.vertx.core.Context;
import io.vertx.core.json.JsonObject;

@FunctionalInterface
//TODO REMOVE
public interface ThreeParamFunction<T> {

  T apply(List<JsonObject> a, Request b, Context c);

}

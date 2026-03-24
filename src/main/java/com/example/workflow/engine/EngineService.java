package com.example.workflow.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

@Component
public class EngineService {

  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final RestTemplate http;
  private final boolean enabled;
  private final long stepSleepMs;

  public EngineService(JdbcTemplate jdbc, ObjectMapper mapper,
                       @Value("${engine.enabled:true}") boolean enabled,
                       @Value("${engine.step-sleep-ms:2000}") long stepSleepMs) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.http = new RestTemplate();
    this.enabled = enabled;
    this.stepSleepMs = stepSleepMs;
  }

  // Work item claimed from DB
  record StepInstance(UUID id, UUID runId, UUID stepDefId) {}

  // Engine tick
  @Scheduled(fixedDelayString = "${engine.tick-ms:500}")
  public void tick() {
    if (!enabled) return;
    try { engineOnce(); } catch (Exception ignore) {}
  }

  // Claim one pending step (skip-locked)
  private StepInstance fetchNextPending() {
    return jdbc.execute((ConnectionCallback<StepInstance>) conn -> {
      conn.setAutoCommit(false);
      try (var ps = conn.prepareStatement(
              "select si.id, si.workflow_run_id, si.step_definition_id " +
                      "  from workflow_step_instance si " +
                      "  join workflow_run r on r.id = si.workflow_run_id " +
                      " where si.status='PENDING' " +
                      "   and (si.next_attempt_at is null or si.next_attempt_at <= now()) " +
                      " order by si.created_at " +
                      " for update skip locked limit 1")) {
        try (var rs = ps.executeQuery()) {
          if (!rs.next()) { conn.commit(); return null; }
          UUID id = (UUID) rs.getObject(1);
          UUID runId = (UUID) rs.getObject(2);
          UUID defId = (UUID) rs.getObject(3);

          try (var upd = conn.prepareStatement(
                  "update workflow_step_instance set status='IN_PROGRESS', started_at=now() where id=?")) {
            upd.setObject(1, id);
            upd.executeUpdate();
          }

          conn.commit();
          return new StepInstance(id, runId, defId);
        }
      } catch (Exception e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    });
  }

  // Fetch step + run row
  private Map<String,Object> getStepAndRun(UUID stepInstanceId) {
    String sql = """
            select si.id as si_id, si.workflow_run_id, si.step_definition_id, si.status,
                   sd.step_name, sd.step_type, sd.service_endpoint, sd.method, sd.config as step_config,
                   r.trigger_payload, r.workflow_id
              from workflow_step_instance si
              join workflow_step_definition sd on sd.id = si.step_definition_id
              join workflow_run r on r.id = si.workflow_run_id
             where si.id = ?
        """;
    return jdbc.queryForMap(sql, stepInstanceId);
  }

  // Parse any DB cell/object to JsonNode: PGobject(jsonb) | String | POJO
  private JsonNode asJson(Object obj) {
    try {
      if (obj == null) return null;
      if (obj instanceof PGobject pg && "jsonb".equalsIgnoreCase(pg.getType())) {
        return mapper.readTree(pg.getValue());
      }
      if (obj instanceof String s) {
        return mapper.readTree(s);
      }
      return mapper.valueToTree(obj);
    } catch (Exception e) {
      return null;
    }
  }

  // Convert a JsonNode object to Map<String,Object> (for context maps)
  private Map<String,Object> nodeToMap(JsonNode node) {
    if (node == null || node.isNull()) return null;
    return mapper.convertValue(node, Map.class);
  }

  // Build execution context: { trigger, steps:{ <name>:{ response: <json-map> } } }
  private Map<String,Object> buildContext(UUID runId) {
    Map<String,Object> ctx = new HashMap<>();

    // Parse trigger_payload jsonb -> JsonNode -> Map (NOT wrapper)
    Map<String,Object> run = jdbc.queryForMap("select trigger_payload from workflow_run where id=?", runId);
    JsonNode trigNode = asJson(run.get("trigger_payload"));
    Map<String,Object> triggerMap = nodeToMap(trigNode);
    ctx.put("trigger", triggerMap);

    // Parse DONE step responses jsonb -> Map
    List<Map<String,Object>> rows = jdbc.queryForList("""
            select sd.step_name, si.response_payload
              from workflow_step_instance si
              join workflow_step_definition sd on sd.id = si.step_definition_id
             where si.workflow_run_id = ? and si.status = 'DONE'
        """, runId);

    Map<String,Object> steps = new HashMap<>();
    for (var r: rows) {
      String name = (String) r.get("step_name");
      JsonNode respNode = asJson(r.get("response_payload"));
      Map<String,Object> v = new HashMap<>();
      v.put("response", nodeToMap(respNode));
      steps.put(name, v);
    }
    ctx.put("steps", steps);
    return ctx;
  }

  // Resolve "$.a.b" from a map context (non-regex; avoids escaping issues)
  private Object getByPath(Object obj, String path) {
    if (obj == null || path == null) return null;
    String p = path.startsWith("$.") ? path.substring(2) : path;
    String[] parts = p.split("\\.");
    Object cur = obj;
    for (String k : parts) {
      if (!(cur instanceof Map<?,?> m)) return null;
      cur = m.get(k);
      if (cur == null) return null;
    }
    return cur;
  }

  private double toNumber(Object v){
    if (v == null) return Double.NaN;
    if (v instanceof Number n) return n.doubleValue();
    try { return new BigDecimal(v.toString()).doubleValue(); }
    catch(Exception e){ return Double.NaN; }
  }

  // --- Step Executors -------------------------------------------------------

  // MATH: supports const/path inputs, precision, divideByZero strategy; returns {"value": <number|null>}
  private Map<String,Object> executeMath(Map<String,Object> stepRow, Map<String,Object> context) throws Exception {
    JsonNode cfg  = asJson(stepRow.get("step_config"));
    JsonNode math = (cfg != null) ? cfg.get("math") : null;
    String op = (math != null && math.hasNonNull("op"))
            ? math.get("op").asText().trim().toUpperCase(Locale.ROOT)
            : null;
    if (op == null || op.isBlank()) throw new RuntimeException("UnsupportedOp:null");

    List<Double> args = new ArrayList<>();
    if (math != null && math.has("inputs")) {
      for (JsonNode inp : math.get("inputs")) {
        if (inp.has("const")) {
          JsonNode c = inp.get("const");
          double val;
          if (c.isNumber())       val = c.doubleValue();
          else if (c.isTextual()) val = toNumber(c.asText());
          else                    val = Double.NaN;
          args.add(val);
        } else if (inp.has("path")) {
          args.add(toNumber(getByPath(context, inp.get("path").asText())));
        } else {
          args.add(Double.NaN);
        }
      }
    }

    if (args.stream().anyMatch(a -> a == null || Double.isNaN(a))) {
      throw new RuntimeException("InvalidNumber");
    }

    JsonNode onError = (cfg != null) ? cfg.get("onError") : null;
    Double value;

    switch (op) {
      case "ADD" -> value = args.stream().reduce(0.0, Double::sum);
      case "SUB" -> value = args.get(0) - args.get(1);
      case "MUL" -> value = args.stream().reduce(1.0, (a,b)->a*b);
      case "DIV" -> {
        double a = args.get(0), b = args.get(1);
        if (b == 0.0) {
          String mode = (onError != null && onError.has("divideByZero"))
                  ? onError.get("divideByZero").asText()
                  : "FAIL";
          switch (mode) {
            case "RETURN_ZERO" -> value = 0.0;
            case "RETURN_NULL" -> {
              Map<String,Object> out = new HashMap<>();
              out.put("value", null);
              return out;
            }
            default -> throw new RuntimeException("DivideByZero");
          }
        } else value = a / b;
      }
      case "MOD" -> {
        double a = args.get(0), b = args.get(1);
        if (b == 0.0) {
          String mode = (onError != null && onError.has("divideByZero"))
                  ? onError.get("divideByZero").asText()
                  : "FAIL";
          switch (mode) {
            case "RETURN_ZERO" -> value = 0.0;
            case "RETURN_NULL" -> {
              Map<String,Object> out = new HashMap<>();
              out.put("value", null);
              return out;
            }
            default -> throw new RuntimeException("ModuloByZero");
          }
        } else value = a % b;
      }
      case "POW" -> value = Math.pow(args.get(0), args.get(1));
      case "MIN" -> value = args.stream().min(Double::compare).orElse(Double.NaN);
      case "MAX" -> value = args.stream().max(Double::compare).orElse(Double.NaN);
      case "AVG" -> value = args.stream().mapToDouble(d -> d).average().orElse(Double.NaN);
      case "ABS" -> value = Math.abs(args.get(0));
      default -> throw new RuntimeException("UnsupportedOp:" + op);
    }

    Integer precision = (math != null && math.has("precision") && !math.get("precision").isNull())
            ? math.get("precision").asInt()
            : null;
    if (precision != null && value != null) {
      double f = Math.pow(10, precision);
      value = Math.round(value * f) / f;
    }

    Map<String,Object> result = new HashMap<>();
    result.put("value", value);
    return result;
  }

  // API_CALL: headers + requestMap; supports $.path for fields
  private Object executeApiCall(Map<String,Object> stepRow, Map<String,Object> context) throws Exception {
    JsonNode cfg = asJson(stepRow.get("step_config"));
    String url = (String) stepRow.get("service_endpoint");
    String method = Optional.ofNullable((String) stepRow.get("method"))
            .orElse("POST")
            .toUpperCase(Locale.ROOT);

    if (url == null || url.isBlank()) {
      return Map.of("ok", true, "info", "no-op (no endpoint)");
    }

    Map<String,String> headers = new HashMap<>();
    if (cfg != null && cfg.has("headers")) {
      Iterator<String> it = cfg.get("headers").fieldNames();
      while (it.hasNext()) {
        String k = it.next();
        headers.put(k, cfg.get("headers").get(k).asText());
      }
    }

    Map<String,Object> payload;
    if (cfg != null && cfg.has("requestMap")) {
      payload = new HashMap<>();
      Iterator<String> it = cfg.get("requestMap").fieldNames();
      while (it.hasNext()) {
        String k = it.next();
        JsonNode v = cfg.get("requestMap").get(k);
        if (v.isTextual() && v.asText().startsWith("$."))
          payload.put(k, getByPath(context, v.asText()));
        else if (v.isNumber())
          payload.put(k, v.numberValue());
        else
          payload.put(k, v.asText());
      }
    } else {
      @SuppressWarnings("unchecked")
      Map<String,Object> trig = (Map<String,Object>) context.get("trigger");
      payload = trig;
    }

    org.springframework.http.HttpHeaders h = new org.springframework.http.HttpHeaders();
    headers.forEach(h::add);
    var entity = new org.springframework.http.HttpEntity<>(payload, h);

    return switch (method) {
      case "GET"    -> http.exchange(url, org.springframework.http.HttpMethod.GET,    entity, Object.class, payload).getBody();
      case "DELETE" -> http.exchange(url, org.springframework.http.HttpMethod.DELETE, entity, Object.class).getBody();
      case "PUT"    -> http.exchange(url, org.springframework.http.HttpMethod.PUT,    entity, Object.class).getBody();
      case "PATCH"  -> http.exchange(url, org.springframework.http.HttpMethod.PATCH,  entity, Object.class).getBody();
      default       -> http.postForObject(url, entity, Object.class);
    };
  }

  // --- Retry / Scheduling helpers ------------------------------------------

  private Date computeNextAttemptAt(int retryCount, JsonNode retries) {
    int maxAttempts     = (retries != null && retries.has("maxAttempts"))     ? retries.get("maxAttempts").asInt()     : 4;
    int initialDelaySec = (retries != null && retries.has("initialDelaySec")) ? retries.get("initialDelaySec").asInt() : 2;
    int maxBackoffSec   = (retries != null && retries.has("maxBackoffSec"))   ? retries.get("maxBackoffSec").asInt()   : 60;
    int jitterMs        = (retries != null && retries.has("jitterMs"))        ? retries.get("jitterMs").asInt()        : 250;

    double base   = Math.pow(2, Math.max(0, retryCount));
    long delayMs  = Math.min(maxBackoffSec, (long)(base * initialDelaySec)) * 1000L;
    long jitter   = (long)(Math.random() * jitterMs);
    return new Date(System.currentTimeMillis() + delayMs + jitter);
  }

  private PGobject toJsonb(Object o) throws Exception {
    PGobject j = new PGobject();
    j.setType("jsonb");
    j.setValue(o == null ? null : mapper.writeValueAsString(o));
    return j;
  }

  private void markRunIfDone(UUID runId) {
    Map<String,Object> counts = jdbc.queryForMap("""
            select count(*) filter (where status in ('PENDING','IN_PROGRESS')) as open_cnt,
                   count(*) filter (where status='FAILED') as failed_cnt
              from workflow_step_instance
             where workflow_run_id=?
        """, runId);

    long open = ((Number)counts.get("open_cnt")).longValue();
    long failed = ((Number)counts.get("failed_cnt")).longValue();

    if (open == 0) {
      String status = failed > 0 ? "FAILED" : "COMPLETED";
      jdbc.update("update workflow_run set status=? where id=?", status, runId);
    }
  }

  // --- Transition enqueue ---------------------------------------------------

  private void enqueueNextSteps(UUID fromStepDefId, UUID runId, Map<String,Object> context) throws Exception {
    List<Map<String,Object>> rows = jdbc.queryForList("""
            select t.id, t.to_step, t.condition, sd.step_name, sd.step_type
              from workflow_step_transition t
              join workflow_step_definition sd on sd.id = t.to_step
             where t.from_step = ?
        """, fromStepDefId);

    for (var t : rows) {
      Object condRaw = t.get("condition");
      Map<String,Object> cond = null;
      try {
        if (condRaw instanceof PGobject pg && "jsonb".equalsIgnoreCase(pg.getType())) {
          cond = mapper.readValue(pg.getValue(), Map.class);
        } else if (condRaw instanceof String s) {
          cond = mapper.readValue(s, Map.class);
        } else if (condRaw instanceof Map<?,?> m) {
          @SuppressWarnings("unchecked")
          Map<String,Object> mm = (Map<String,Object>) m;
          cond = mm;
        }
      } catch (Exception ignore) {
        cond = null;
      }

      boolean pass = true;
      try {
        if (cond != null) {
          if (cond.containsKey("always")) {
            pass = Boolean.TRUE.equals(cond.get("always"));
          } else if (cond.containsKey("exists")) {
            @SuppressWarnings("unchecked")
            Map<String,Object> ex = (Map<String,Object>) cond.get("exists");
            String p = (String) ex.get("path");
            pass = getByPath(context, p) != null;
          }
        }
      } catch (Exception ignore) {
        pass = false;
      }

      if (pass) {
        jdbc.update(
                "insert into workflow_step_instance (workflow_run_id, step_definition_id, status, request_payload) values (?,?, 'PENDING', ?)",
                runId,
                (UUID)t.get("to_step"),
                toJsonb(context.get("trigger"))
        );
      }
    }
  }

  // --- One engine cycle -----------------------------------------------------

  private void engineOnce() throws Exception {
    StepInstance item = fetchNextPending();
    if (item == null) return;

    Map<String,Object> row = getStepAndRun(item.id());
    Map<String,Object> context = buildContext(item.runId());

    try {
      String type = ((String)row.get("step_type")).toUpperCase(Locale.ROOT);
      Object response = switch (type) {
        case "MATH"      -> executeMath(row, context);
        case "API_CALL"  -> executeApiCall(row, context);
        case "WAIT_EVENT"-> Map.of("ok", true);
        case "APPROVAL"  -> Map.of("status", "APPROVED");
        default          -> Map.of("ok", true);
      };

      jdbc.update("""
                update workflow_step_instance
                   set status='DONE', response_payload=?, finished_at=now()
                 where id=?
            """, toJsonb(response), item.id());

      // Small sleep for visible progress in UI
      try { Thread.sleep(stepSleepMs); } catch (InterruptedException ignore) {}

      enqueueNextSteps(item.stepDefId(), item.runId(), context);

    } catch (Exception ex) {
      // SAFE retry handling via JsonNode (no casts to Map)
      JsonNode cfg = asJson(row.get("step_config"));
      JsonNode retries = (cfg != null) ? cfg.get("retries") : null;

      Integer current = jdbc.queryForObject(
              "select retry_count from workflow_step_instance where id=?",
              Integer.class, item.id()
      );
      if (current == null) current = 0;

      int maxAttempts = (retries != null && retries.has("maxAttempts"))
              ? retries.get("maxAttempts").asInt()
              : 4;

      if (current >= maxAttempts) {
        jdbc.update("""
                    update workflow_step_instance
                       set status='FAILED',
                           response_payload=?,
                           last_error=?,
                           finished_at=now()
                     where id=?
                """,
                toJsonb(Map.of("error", ex.getMessage() == null ? "error" : ex.getMessage())),
                ex.toString(),
                item.id());

      } else {
        Date nextAt = computeNextAttemptAt(current, retries);
        jdbc.update("""
                    update workflow_step_instance
                       set status='PENDING',
                           retry_count = retry_count + 1,
                           last_error = ?,
                           next_attempt_at = ?
                     where id=?
                """,
                ex.toString(),
                new Timestamp(nextAt.getTime()),
                item.id());
      }

    } finally {
      markRunIfDone(item.runId());
    }
  }
}
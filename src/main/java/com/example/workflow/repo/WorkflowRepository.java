
package com.example.workflow.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.UUID;

@Repository
public class WorkflowRepository {
  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public WorkflowRepository(JdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc; this.mapper = mapper;
  }

  // Workflows
  public List<Map<String,Object>> listWorkflows(){
    return jdbc.queryForList("select * from workflow_definition order by created_at desc");
  }
  public Map<String,Object> createWorkflow(String name, String description){
    return jdbc.queryForMap("insert into workflow_definition (name, description) values (?,?) returning *", name, description);
  }
  public int updateWorkflow(UUID id, String name, String description){
    return jdbc.update("update workflow_definition set name=?, description=? where id=?", name, description, id);
  }
  public int deleteWorkflow(UUID id){ return jdbc.update("delete from workflow_definition where id=?", id); }
  public void resetWorkflow(UUID id){
    jdbc.update("delete from workflow_step_transition where from_step in (select id from workflow_step_definition where workflow_id=?)", id);
    jdbc.update("delete from workflow_step_definition where workflow_id=?", id);
  }

  // Definition
  public Map<String,Object> getDefinition(UUID workflowId){
    var wf = jdbc.queryForList("select * from workflow_definition where id=?", workflowId);
    if (wf.isEmpty()) return null;
    var steps = jdbc.queryForList("select * from workflow_step_definition where workflow_id=? order by created_at", workflowId);
    var trans = jdbc.queryForList("select * from workflow_step_transition where from_step in (select id from workflow_step_definition where workflow_id=?)", workflowId);
    Map<String,Object> out = new HashMap<>(); out.put("workflow", wf.get(0)); out.put("steps", steps); out.put("transitions", trans); return out;
  }

  // Steps
  public Map<String,Object> addStep(UUID workflowId, String name, String type, String endpoint, String method, Object config, boolean isStart, boolean isEnd) throws Exception {
    PGobject jsonb = new PGobject(); jsonb.setType("jsonb"); jsonb.setValue(config==null? null : mapper.writeValueAsString(config));
    return jdbc.queryForMap("insert into workflow_step_definition (workflow_id, step_name, step_type, service_endpoint, method, config, is_start, is_end) values (?,?,?,?,?,?,?,?) returning *",
      workflowId, name, type, endpoint, method, jsonb, isStart, isEnd);
  }
  public int updateStep(UUID stepId, String name, String type, String endpoint, String method, Object config, Boolean isStart, Boolean isEnd) throws Exception {
    PGobject jsonb = new PGobject(); jsonb.setType("jsonb"); jsonb.setValue(config==null? null : mapper.writeValueAsString(config));
    return jdbc.update("update workflow_step_definition set step_name=?, step_type=?, service_endpoint=?, method=?, config=?, is_start=?, is_end=? where id=?",
      name, type, endpoint, method, jsonb, isStart, isEnd, stepId);
  }
  public int deleteStep(UUID stepId){
    jdbc.update("delete from workflow_step_transition where from_step=? or to_step=?", stepId, stepId);
    return jdbc.update("delete from workflow_step_definition where id=?", stepId);
  }

  // Transitions
  public Map<String,Object> addTransition(UUID fromStep, UUID toStep, Object condition) throws Exception {
    PGobject jsonb = new PGobject(); jsonb.setType("jsonb"); jsonb.setValue(condition==null? null : mapper.writeValueAsString(condition));
    return jdbc.queryForMap("insert into workflow_step_transition (from_step, to_step, condition) values (?,?,?) returning *", fromStep, toStep, jsonb);
  }
  public int deleteTransition(UUID transitionId){ return jdbc.update("delete from workflow_step_transition where id=?", transitionId); }

  // Runs
  public List<Map<String,Object>> listRunSummary(UUID workflowId){
    String where = workflowId!=null? " where r.workflow_id = ?" : "";
    Object[] args = workflowId!=null? new Object[]{workflowId} : new Object[]{};
    return jdbc.queryForList("""
      select r.id as run_id,
             r.created_at as run_created_at,
             w.name as workflow_name,
             sd.step_name,
             si.started_at,
             si.finished_at,
             si.status,
             si.response_payload
        from workflow_run r
        join workflow_definition w on w.id = r.workflow_id
        join workflow_step_instance si on si.workflow_run_id = r.id
        join workflow_step_definition sd on sd.id = si.step_definition_id
      """ + where + " order by r.created_at desc, si.started_at nulls first", args);
  }
  public Map<String,Object> getRunDetail(UUID runId){
    var run = jdbc.queryForList("select * from workflow_run where id=?", runId);
    if (run.isEmpty()) return null;
    var steps = jdbc.queryForList("""
      select si.*, sd.step_name, sd.step_type
        from workflow_step_instance si
        join workflow_step_definition sd on sd.id = si.step_definition_id
       where si.workflow_run_id=?
       order by si.created_at
    """, runId);
    Map<String,Object> out = new HashMap<>(); out.put("run", run.get(0)); out.put("steps", steps); return out;
  }
  public List<Map<String,Object>> listRunsForWorkflow(UUID workflowId){
    return jdbc.queryForList("select id, status, created_at from workflow_run where workflow_id=? order by created_at desc", workflowId);
  }

  // Trigger
  public Map<String,Object> trigger(UUID workflowId, Object triggerPayload) throws Exception {
    var starts = jdbc.queryForList("select id from workflow_step_definition where workflow_id=? and is_start=true", workflowId);
    if (starts.isEmpty()) throw new RuntimeException("No start step defined");
    PGobject jsonb = new PGobject(); jsonb.setType("jsonb"); jsonb.setValue(triggerPayload==null? null : mapper.writeValueAsString(triggerPayload));
    var run = jdbc.queryForMap("insert into workflow_run (workflow_id, trigger_payload) values (?,?) returning *", workflowId, jsonb);
    for (var s: starts){
      jdbc.update("insert into workflow_step_instance (workflow_run_id, step_definition_id, status, request_payload) values (?,?, 'PENDING', ?)",
        run.get("id"), s.get("id"), jsonb);
    }
    return run;
  }
}

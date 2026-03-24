package com.example.workflow.controller;

import com.example.workflow.repo.WorkflowRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.UUID;

@RestController
@RequestMapping("/api/admin")
public class AdminController {
  private final WorkflowRepository repo;
  public AdminController(WorkflowRepository repo) { this.repo = repo; }

  // Workflows
  @GetMapping("/workflows") public List<Map<String,Object>> listWorkflows(){ return repo.listWorkflows(); }
  @PostMapping("/workflows") public Map<String,Object> createWorkflow(@RequestBody Map<String,Object> body){ return repo.createWorkflow((String)body.get("name"),(String)body.getOrDefault("description",null)); }
  @PutMapping("/workflows/{id}") public ResponseEntity<?> updateWorkflow(@PathVariable("id") UUID id, @RequestBody Map<String,Object> body){ int n=repo.updateWorkflow(id,(String)body.get("name"),(String)body.getOrDefault("description",null)); return n>0? ResponseEntity.ok(Map.of("updated",true)) : ResponseEntity.notFound().build(); }
  @PostMapping("/workflows/{id}/reset") public Map<String,Object> reset(@PathVariable("id") UUID id){ repo.resetWorkflow(id); return Map.of("reset",true); }
  @DeleteMapping("/workflows/{id}") public Map<String,Object> delete(@PathVariable("id") UUID id){ return Map.of("deleted", repo.deleteWorkflow(id)>0); }

  @GetMapping("/workflows/{id}/definition") public ResponseEntity<?> def(@PathVariable("id") UUID id){ var d=repo.getDefinition(id); return d==null? ResponseEntity.notFound().build(): ResponseEntity.ok(d); }

  // Steps
  @PostMapping("/workflows/{id}/steps") public Map<String,Object> addStep(@PathVariable("id") UUID id, @RequestBody Map<String,Object> b) throws Exception { return repo.addStep(id,(String)b.get("step_name"),(String)b.get("step_type"),(String)b.getOrDefault("service_endpoint",null),(String)b.getOrDefault("method",null),b.get("config"),Boolean.TRUE.equals(b.get("is_start")),Boolean.TRUE.equals(b.get("is_end"))); }
  @PutMapping("/workflows/{workflowId}/steps/{stepId}") public Map<String,Object> updStep(@PathVariable("workflowId") UUID workflowId,@PathVariable("stepId") UUID stepId,@RequestBody Map<String,Object> b) throws Exception { return Map.of("updated", repo.updateStep(stepId,(String)b.get("step_name"),(String)b.get("step_type"),(String)b.getOrDefault("service_endpoint",null),(String)b.getOrDefault("method",null),b.get("config"),(Boolean)b.getOrDefault("is_start",false),(Boolean)b.getOrDefault("is_end",false))>0); }
  @DeleteMapping("/workflows/{workflowId}/steps/{stepId}") public Map<String,Object> delStep(@PathVariable("workflowId") UUID workflowId,@PathVariable("stepId") UUID stepId){ return Map.of("deleted", repo.deleteStep(stepId)>0); }

  // Transitions
  @PostMapping("/workflows/{id}/transitions") public Map<String,Object> addTr(@PathVariable("id") UUID id,@RequestBody Map<String,Object> b) throws Exception { return repo.addTransition(UUID.fromString((String)b.get("from_step")), UUID.fromString((String)b.get("to_step")), b.getOrDefault("condition", Map.of("always",true))); }
  @DeleteMapping("/workflows/{workflowId}/transitions/{transitionId}") public Map<String,Object> delTr(@PathVariable("workflowId") UUID workflowId,@PathVariable("transitionId") UUID transitionId){ return Map.of("deleted", repo.deleteTransition(transitionId)>0); }

  // Runs & trigger
  @PostMapping("/workflows/{id}/trigger") public Map<String,Object> trigger(@PathVariable("id") UUID id, @RequestBody(required=false) Map<String,Object> payload) throws Exception { return repo.trigger(id, payload); }

  @GetMapping("/runs") public List<Map<String,Object>> runs(@RequestParam(name="workflowId", required=false) UUID workflowId){
    return repo.listRunSummary(workflowId).stream().map(r->{ Object payload=r.get("response_payload"); Object value=null; if(payload instanceof Map<?,?> m && m.containsKey("value")) value=m.get("value"); r.put("result", value!=null? value: payload); r.remove("response_payload"); return r; }).collect(Collectors.toList()); }
  @GetMapping("/runs/{runId}") public ResponseEntity<?> runDetail(@PathVariable("runId") UUID runId){ var d=repo.getRunDetail(runId); return d==null? ResponseEntity.notFound().build() : ResponseEntity.ok(d); }
  @GetMapping("/workflows/{id}/runs") public List<Map<String,Object>> runsOfWorkflow(@PathVariable("id") UUID id){ return repo.listRunsForWorkflow(id); }
}

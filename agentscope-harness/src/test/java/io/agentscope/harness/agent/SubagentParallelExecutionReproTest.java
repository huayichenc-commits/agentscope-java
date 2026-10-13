/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.agentscope.harness.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.agentscope.core.agent.RuntimeContext;
import io.agentscope.core.event.AgentEvent;
import io.agentscope.core.event.AgentEventType;
import io.agentscope.core.event.AgentStartEvent;
import io.agentscope.core.message.Msg;
import io.agentscope.core.message.MsgRole;
import io.agentscope.core.message.TextBlock;
import io.agentscope.core.message.ToolUseBlock;
import io.agentscope.core.model.ChatResponse;
import io.agentscope.core.model.Model;
import io.agentscope.core.util.JsonUtils;
import io.agentscope.harness.agent.filesystem.local.LocalFilesystem;
import io.agentscope.harness.agent.testing.HarnessQuiescence;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Reproduction for #3525 on current main: two PARALLEL spawns of the SAME subagent type. Captures
 * the forwarded events' {@code execution} identity to answer whether children of the same type
 * stay attributable per instance (distinct {@code execution.sessionId}) without any
 * {@code metadata.childSessionId} tag.
 */
@HarnessQuiescence
class SubagentParallelExecutionReproTest {

    @TempDir Path workspace;
    @TempDir Path stateHome;

    private String previousStateHome;
    private HarnessAgent parent;

    @BeforeEach
    void overrideStateHome() {
        previousStateHome = System.getProperty("agentscope.state.home");
        System.setProperty("agentscope.state.home", stateHome.toString());
    }

    @AfterEach
    void tearDown() {
        try {
            if (parent != null) {
                parent.close();
            }
        } finally {
            if (previousStateHome != null) {
                System.setProperty("agentscope.state.home", previousStateHome);
            } else {
                System.clearProperty("agentscope.state.home");
            }
        }
    }

    private static ChatResponse textChunk(String id, String text) {
        return new ChatResponse(
                id, List.of(TextBlock.builder().text(text).build()), null, Map.of(), null);
    }

    private static ChatResponse stopChunk(String id, String text) {
        return new ChatResponse(
                id, List.of(TextBlock.builder().text(text).build()), null, Map.of(), "stop");
    }

    private static ToolUseBlock spawnCall(String id, String agentId, String task) {
        Map<String, Object> in = Map.of("agent_id", agentId, "task", task, "timeout_seconds", 60);
        String contentJson = JsonUtils.getJsonCodec().toJson(in);
        return ToolUseBlock.builder()
                .id("tc-" + id)
                .name("agent_spawn")
                .input(in)
                .content(contentJson)
                .build();
    }

    private void writeSubagentSpec(String childId, String description, String body)
            throws Exception {
        Files.createDirectories(workspace.resolve("subagents"));
        Files.writeString(
                workspace.resolve("subagents/" + childId + ".md"),
                "---\ndescription: " + description + "\n---\n" + body + "\n");
    }

    @Test
    void parallelSameTypeChildren_forwardedEventsCarryDistinctChildExecution() throws Exception {
        writeSubagentSpec("researcher", "Research specialist", "You are a researcher.");

        Model model = mock(Model.class);
        when(model.getModelName()).thenReturn("stub");
        when(model.stream(anyList(), any(), any()))
                .thenAnswer(
                        inv -> {
                            List<Msg> msgs = inv.getArgument(0);
                            String json = JsonUtils.getJsonCodec().toJson(msgs);
                            if (json.contains("You are a researcher.")) {
                                // child turn: distinguish the two instances by their task text
                                if (json.contains("TASK_ALPHA")) {
                                    return Flux.just(
                                            textChunk("ca", "alpha delta 1"),
                                            textChunk("ca", "alpha delta 2"),
                                            stopChunk("ca", "alpha done"));
                                }
                                return Flux.just(
                                        textChunk("cb", "beta delta 1"),
                                        textChunk("cb", "beta delta 2"),
                                        stopChunk("cb", "beta done"));
                            }
                            // parent turn: first call spawns two PARALLEL same-type children;
                            // later call (tool results in history) summarises
                            if (json.contains("alpha done") || json.contains("beta done")) {
                                return Flux.just(stopChunk("p2", "all done"));
                            }
                            return Flux.just(
                                    new ChatResponse(
                                            "p1",
                                            List.of(
                                                    spawnCall("a", "researcher", "TASK_ALPHA"),
                                                    spawnCall("b", "researcher", "TASK_BETA")),
                                            null,
                                            Map.of(),
                                            "tool_use"));
                        });

        parent =
                HarnessAgent.builder()
                        .name("parent")
                        .model(model)
                        .workspace(workspace)
                        .abstractFilesystem(new LocalFilesystem(workspace))
                        .build();

        RuntimeContext ctx = RuntimeContext.builder().sessionId("sess-parallel-repro").build();
        List<AgentEvent> events =
                parent.streamEvents(
                                List.of(
                                        Msg.builder()
                                                .role(MsgRole.USER)
                                                .textContent("start")
                                                .build()),
                                ctx)
                        .collectList()
                        .block();

        assertNotNull(events);

        // 1) Two wrapper AGENT_START bookends with distinct child session ids
        List<AgentStartEvent> childStarts =
                events.stream()
                        .filter(
                                e ->
                                        e.getType() == AgentEventType.AGENT_START
                                                && e.getSource() != null)
                        .map(e -> (AgentStartEvent) e)
                        .collect(Collectors.toList());
        assertEquals(
                2,
                childStarts.size(),
                "expected 2 parallel child AGENT_START; got: "
                        + events.stream()
                                .map(e -> e.getType() + "(src=" + e.getSource() + ")")
                                .collect(Collectors.joining(", ")));
        Set<String> childSessions = new HashSet<>();
        childStarts.forEach(s -> childSessions.add(s.getSessionId()));
        assertEquals(2, childSessions.size(), "child session ids must be distinct per instance");

        // 2) Every forwarded non-bookend event carries an execution identity that is one of the
        //    two CHILD sessions — never null, never the parent session
        List<AgentEvent> forwarded =
                events.stream()
                        .filter(
                                e ->
                                        e.getSource() != null
                                                && e.getType() != AgentEventType.AGENT_START
                                                && e.getType() != AgentEventType.AGENT_END)
                        .collect(Collectors.toList());
        assertFalse(forwarded.isEmpty(), "expected forwarded child events");
        Set<String> seenSessions = new HashSet<>();
        for (AgentEvent e : forwarded) {
            assertNotNull(
                    e.getExecution(),
                    "forwarded event without execution: " + e.getType() + " src=" + e.getSource());
            String sid = e.getExecution().sessionId();
            assertNotNull(sid, "execution.sessionId null on " + e.getType());
            assertTrue(
                    childSessions.contains(sid),
                    "execution.sessionId is neither child session (parent leak?): "
                            + sid
                            + " on "
                            + e.getType());
            seenSessions.add(sid);
        }
        assertEquals(
                childSessions,
                seenSessions,
                "both parallel instances must be attributable among forwarded events");

        // 3) Payload capture for the issue thread: one text delta + one model-call start per
        //    instance, plus the bookends' fields
        System.out.println("=== #3525 PARALLEL SAME-TYPE REPRO (current main) ===");
        System.out.println("parent session = " + ctx.getSessionId());
        for (AgentStartEvent s : childStarts) {
            System.out.println("BOOKEND AGENT_START: " + JsonUtils.getJsonCodec().toJson(s));
        }
        for (String sid : childSessions) {
            events.stream()
                    .filter(
                            e ->
                                    e.getSource() != null
                                            && e.getExecution() != null
                                            && sid.equals(e.getExecution().sessionId())
                                            && (e.getType() == AgentEventType.TEXT_BLOCK_DELTA
                                                    || e.getType()
                                                            == AgentEventType.MODEL_CALL_START))
                    .findFirst()
                    .ifPresent(
                            e ->
                                    System.out.println(
                                            "SAMPLE "
                                                    + e.getType()
                                                    + " (execution.sessionId="
                                                    + sid
                                                    + "):"
                                                    + " "
                                                    + JsonUtils.getJsonCodec().toJson(e)));
        }
    }
}

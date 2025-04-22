package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.CompilationThreadPool;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;


public class SolutionThread extends UserThread {

    // Caches
    private static final Map<Long, CompiledMethodInfo> globalCachedInfo = new HashMap<>();
    private static final ThreadLocal<Map<Long, CompiledMethodInfo>> privateCachedInfo = ThreadLocal.withInitial(HashMap::new);

    // Hotness
    private static final Map<Long, Long> globalHotness = new HashMap<>();
    private static final ThreadLocal<Map<Long, Long>> privateHotness = ThreadLocal.withInitial(HashMap::new);
    private final Map<Long, Long> localHotness = new HashMap<>();

    // Interpretations
    private static final Map<Long, Long> globalInterpretationCounter = new HashMap<>();
    private static final ThreadLocal<Map<Long, Long>> privateInterpretationCounter
            = ThreadLocal.withInitial(HashMap::new);
    private final Map<Long, Long> localInterpretationCounter = new HashMap<>();


    private long getHotness(long id) {
        return privateHotness.get().getOrDefault(id, 0L);
    }

    private void incrementHotness(long id) {
        privateHotness.get().put(id, privateHotness.get().getOrDefault(id, 0L) + 1);
        localHotness.put(id, localHotness.getOrDefault(id, 0L) + 1);
        if (localHotness.getOrDefault(id, 0L) > 1000) {
            globalHotness.put(id, globalHotness.getOrDefault(id, 0L) + localHotness.getOrDefault(id, 0L));
            privateHotness.get().put(id, globalHotness.get(id));
            localHotness.remove(id);
        }
    }

    private long getInterpretations(long id) {
        return privateInterpretationCounter.get().getOrDefault(id, 0L);
    }

    private void incrementInterpretations(long id) {
        privateInterpretationCounter.get().put(id, privateInterpretationCounter.get().getOrDefault(id, 0L) + 1);
        localInterpretationCounter.put(id, localInterpretationCounter.getOrDefault(id, 0L) + 1);
        if (localInterpretationCounter.getOrDefault(id, 0L) > 1000) {
            globalInterpretationCounter.put(id,
                    globalInterpretationCounter.getOrDefault(id, 0L) + localInterpretationCounter.getOrDefault(id, 0L));
            privateInterpretationCounter.get().put(id, globalInterpretationCounter.get(id));
            localInterpretationCounter.remove(id);
        }
    }


    private static CompilationThreadPool compilationThreadPool;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        if (compilationThreadPool == null) {
            compilationThreadPool = new CompilationThreadPool(compiler, this, compilationThreadBound);
        }
    }

    public void setCachedInfo(long id, CompilationLevel compilationLevel, CompiledMethod compiledMethod) {
        var payload = new CompiledMethodInfo(compiledMethod, compilationLevel);
        if (!globalCachedInfo.containsKey(id) ||
                (globalCachedInfo.containsKey(id) &&
                        compilationLevel.ordinal() > globalCachedInfo.get(id).compilationLevel.ordinal())) {
            globalCachedInfo.put(id, payload);
        }
    }

    private Optional<CompiledMethodInfo> getCachedInfo(long id) {
        var tlCache = privateCachedInfo.get().get(id);
        var globalCache = globalCachedInfo.get(id);

        if (globalCache == null && tlCache == null) {
            return Optional.empty();
        }

        if (tlCache == null) {
            privateCachedInfo.get().put(id, globalCache);
            return Optional.of(globalCache);
        }

        if (globalCache == null){
            return Optional.of(tlCache);
        }

        CompiledMethodInfo best;
        if (tlCache.compilationLevel.ordinal() > globalCache.compilationLevel.ordinal()){
            best = tlCache;
        } else {
            privateCachedInfo.get().put(id, globalCache);
            best = globalCache;
        }

        return Optional.of(best);
    }

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long methodID = id.id();

        final long hotLevel = getHotness(methodID);
        incrementHotness(methodID);

        Optional<CompiledMethodInfo> possibleMethodInfo = getCachedInfo(methodID);

        if (hotLevel > 10_000 && possibleMethodInfo.isPresent()
                && possibleMethodInfo.get().compilationLevel.ordinal() < CompilationLevel.L2.ordinal()) {
            compilationThreadPool.compile(CompilationLevel.L2, id);

        } else if (hotLevel > 1_000 && possibleMethodInfo.isEmpty()) {
            compilationThreadPool.compile(CompilationLevel.L1, id);
        }



        ExecutionResult execResult;

        if (possibleMethodInfo.isEmpty()) {
            incrementInterpretations(methodID);
            execResult = exec.interpret(id);
        } else {
            execResult = exec.execute(possibleMethodInfo.get().compiledMethod);
        }

        // Synchronization of compilations from thread pool
        if (getInterpretations(methodID) > 9500) {
            // if too many interpretations then we block execution until we get the compiled method from cache
            possibleMethodInfo = getCachedInfo(methodID);
            while (true) {
                var hotness = getHotness(methodID);
                boolean l1_escape = hotness <= 90_000
                        && possibleMethodInfo.isPresent()
                        && possibleMethodInfo.get().compilationLevel == CompilationLevel.L1;

                boolean l2_escape = hotness > 90_000
                        && possibleMethodInfo.isPresent()
                        && possibleMethodInfo.get().compilationLevel == CompilationLevel.L2;

                if (l1_escape || l2_escape) {
                    break;
                }
                possibleMethodInfo = getCachedInfo(methodID);
            }
        }

        return execResult;
    }

    public enum CompilationLevel {
        L1, L2
    }

    private static class CompiledMethodInfo {
        public final CompiledMethod compiledMethod;
        public final CompilationLevel compilationLevel;

        public CompiledMethodInfo(CompiledMethod compiledMethod, CompilationLevel compilationLevel) {
            this.compiledMethod = compiledMethod;
            this.compilationLevel = compilationLevel;
        }

    }


}
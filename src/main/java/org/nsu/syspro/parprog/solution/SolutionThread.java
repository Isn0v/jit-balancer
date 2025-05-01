package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.CompilationThreadPool;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SolutionThread extends UserThread {

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    // Caches
    private static final Map<Long, CompiledMethodInfo> globalCachedInfo = new HashMap<>();
    private static final ThreadLocal<Map<Long, CompiledMethodInfo>> privateCachedInfo = ThreadLocal.withInitial(HashMap::new);

    // Hotness
    private final Map<Long, Long> localHotness = new HashMap<>();


    private long getHotness(long id) {
        return localHotness.getOrDefault(id, 0L);
    }

    private void incrementHotness(long id) {
        localHotness.put(id, localHotness.getOrDefault(id, 0L) + 1);
    }

    private static CompilationThreadPool compilationThreadPool;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        if (compilationThreadPool == null) {
            compilationThreadPool = new CompilationThreadPool(compiler, compilationThreadBound);
        }
    }

    private void setCachedInfo(long id, CompilationLevel compilationLevel, CompiledMethod compiledMethod) {
        try {
            lock.writeLock().lock();
            var payload = new CompiledMethodInfo(compiledMethod, compilationLevel);
            if (!globalCachedInfo.containsKey(id) ||
                    (globalCachedInfo.containsKey(id) &&
                            compilationLevel.ordinal() > globalCachedInfo.get(id).compilationLevel.ordinal())) {
                globalCachedInfo.put(id, payload);
            }
            privateCachedInfo.get().putAll(globalCachedInfo);

        } finally {
            lock.writeLock().unlock();
        }
    }

    private Optional<CompiledMethodInfo> getCachedInfo(long id) {
        return Optional.ofNullable(privateCachedInfo.get().get(id));
    }

    private void updateCachedInfo() {
        try {
            lock.readLock().lock();
            privateCachedInfo.get().putAll(globalCachedInfo);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long methodID = id.id();

        final long hotLevel = getHotness(methodID);
        incrementHotness(methodID);

        Optional<CompiledMethodInfo> possibleMethodInfo = getCachedInfo(methodID);

        Future<CompiledMethod> futureCode = null;
        CompilationLevel futureLevel = null;
        if (hotLevel > 90_000 && possibleMethodInfo.isPresent()
                && possibleMethodInfo.get().compilationLevel.ordinal() < CompilationLevel.L2.ordinal()) {
            futureCode = compilationThreadPool.compile(CompilationLevel.L2, id);
            futureLevel = CompilationLevel.L2;

        } else if (hotLevel > 9_000 && possibleMethodInfo.isEmpty()) {
            futureCode = compilationThreadPool.compile(CompilationLevel.L1, id);
            futureLevel = CompilationLevel.L1;
        }


        ExecutionResult execResult;

        if (possibleMethodInfo.isEmpty()) {
            execResult = exec.interpret(id);
        } else {
            execResult = exec.execute(possibleMethodInfo.get().compiledMethod);
        }

        // Synchronization of compilations from thread pool after fast path
        if (futureCode != null) {
            try {
                var code = futureCode.get();
                if (code != null) {
                    setCachedInfo(methodID, futureLevel, code);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        updateCachedInfo();

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
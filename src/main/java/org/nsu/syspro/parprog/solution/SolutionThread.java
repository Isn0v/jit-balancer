package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.CompilationThreadPool;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class SolutionThread extends UserThread {
    // TODO: Weak-worst-case-latency (think about it)


    private static final Map<Long, CompiledMethodInfo> cachedCompileMethods = new HashMap<>();
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    private static CompilationThreadPool compilationThreadPool;

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        if (compilationThreadPool == null) {
            compilationThreadPool = new CompilationThreadPool(compiler, compilationThreadBound);
        }
    }

    private final Map<Long, Long> hotness = new HashMap<>();

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long methodID = id.id();
        final long hotLevel = hotness.getOrDefault(methodID, 0L);
        hotness.put(methodID, hotLevel + 1);

        Optional<CompiledMethodInfo> possibleMethodInfo = getCachedCompileInfo(methodID);

        if (hotLevel > 90_000 && possibleMethodInfo.isPresent() &&
                possibleMethodInfo.get().compilationLevel.ordinal() < CompilationLevel.L2.ordinal()) {

            final CompiledMethod code = compilationThreadPool.compile(CompilationLevel.L2, id);
            setCachedCompileInfo(new CompiledMethodInfo(code, CompilationLevel.L2));
            return exec.execute(code);

        } else if (hotLevel > 9_000 && possibleMethodInfo.isEmpty()) {
            final CompiledMethod code = compilationThreadPool.compile(CompilationLevel.L1, id);
            setCachedCompileInfo(new CompiledMethodInfo(code, CompilationLevel.L1));
            return exec.execute(code);
        }

        if (possibleMethodInfo.isEmpty()) {
            return exec.interpret(id);
        } else {
            return exec.execute(possibleMethodInfo.get().compiledMethod);
        }
    }

    private static  Optional<CompiledMethodInfo> getCachedCompileInfo(long id) {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(cachedCompileMethods.get(id));
        } finally {
            lock.readLock().unlock();
        }
    }

    public enum CompilationLevel {
        L1,
        L2
    }

    private static void setCachedCompileInfo(CompiledMethodInfo compiledMethodInfo) {
        lock.writeLock().lock();
        try {
            var compiledMethodLevel = compiledMethodInfo.compilationLevel;
            var compiledMethod = compiledMethodInfo.compiledMethod;

            long id = compiledMethod.id().id();
            var possibleCompiledInfo = getCachedCompileInfo(id);

            if (possibleCompiledInfo.isEmpty()) {
                cachedCompileMethods.put(id, new CompiledMethodInfo(compiledMethod, compiledMethodLevel));
                return;
            }

            var prevCompiledMethodLevel = possibleCompiledInfo.get().compilationLevel;

            if (compiledMethodLevel.ordinal() > prevCompiledMethodLevel.ordinal()) {
                cachedCompileMethods.put(id, compiledMethodInfo);
            }
        } finally {
            lock.writeLock().unlock();
        }
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
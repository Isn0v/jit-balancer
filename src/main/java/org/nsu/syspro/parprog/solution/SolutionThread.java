package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.CompilationThread;
import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;


public class SolutionThread extends UserThread {

    private static final Map<Long, CompiledMethodInfo> cachedCompileMethods = new HashMap<>();

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
    }

    private final Map<Long, Long> hotness = new HashMap<>();

    @Override
    public ExecutionResult executeMethod(MethodID id) {
        final long methodID = id.id();
        final long hotLevel = hotness.getOrDefault(methodID, 0L);
        hotness.put(methodID, hotLevel + 1);

        Optional<CompiledMethod> possibleCodeToExec = Optional.empty();
        Optional<CompilationLevel> possibleCompLevel = Optional.empty();

        if (getCachedCompileInfo(methodID).isPresent()) {
            possibleCodeToExec = Optional.ofNullable(getCachedCompileInfo(methodID).get().compiledMethod);
            possibleCompLevel = Optional.ofNullable(getCachedCompileInfo(methodID).get().compilationLevel);
        }

        if (hotLevel > 90_000 && possibleCompLevel.isPresent() &&
                possibleCompLevel.get().ordinal() < CompilationLevel.L2.ordinal()) {

            var compilationThread = getCompilationThread(CompilationLevel.L2, id);
            CompiledMethod code = compilationThread.compile();
            setCachedCompileInfo(new CompiledMethodInfo(code, CompilationLevel.L2));
            return exec.execute(code);

        } else if (hotLevel > 9_000 && possibleCompLevel.isEmpty()) {
            final CompiledMethod code = compiler.compile_l1(id);
            setCachedCompileInfo(new CompiledMethodInfo(code, CompilationLevel.L1));
            return exec.execute(code);

        }

        if (possibleCodeToExec.isEmpty()) {
            return exec.interpret(id);
        } else {
            return exec.execute(possibleCodeToExec.get());
        }
    }

    private static synchronized Optional<CompiledMethodInfo> getCachedCompileInfo(long id) {
        return Optional.ofNullable(cachedCompileMethods.get(id));
    }

    public enum CompilationLevel {
        L1,
        L2
    }

    private static synchronized void setCachedCompileInfo(CompiledMethodInfo compiledMethodInfo) {
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
    }

    private static class CompiledMethodInfo {
        public final CompiledMethod compiledMethod;
        public final CompilationLevel compilationLevel;

        public CompiledMethodInfo(CompiledMethod compiledMethod, CompilationLevel compilationLevel) {
            this.compiledMethod = compiledMethod;
            this.compilationLevel = compilationLevel;
        }

    }


    private static CompilationThread getCompilationThread(CompilationLevel compilationLevel, MethodID methodID) {
        return new CompilationThread(current().compiler, compilationLevel, methodID);
    }

}
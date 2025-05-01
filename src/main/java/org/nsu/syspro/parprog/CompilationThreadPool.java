package org.nsu.syspro.parprog;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SolutionThread.CompilationLevel;

import java.util.HashMap;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CompilationThreadPool {
    private final CompilationEngine compilationEngine;

    private static final HashMap<MethodID, Integer> compile_l1_counter = new HashMap<>();
    private static final HashMap<MethodID, Integer> compile_l2_counter = new HashMap<>();
    private static final Lock lock = new ReentrantLock();

    private final ExecutorService service;

    public CompilationThreadPool(CompilationEngine compilationEngine, int compilationThreadBound) {
        this.compilationEngine = compilationEngine;
        service = Executors.newFixedThreadPool(compilationThreadBound);
    }

    private static void increment(CompilationLevel compilationLevel, MethodID methodID) {
        var counter = switch (compilationLevel) {
            case L1 -> compile_l1_counter;
            case L2 -> compile_l2_counter;
        };

        var current = counter.getOrDefault(methodID, 0) + 1;
        counter.put(methodID, current);
    }

    private static int get(CompilationLevel compilationLevel, MethodID methodID) {
        var counter = switch (compilationLevel) {
            case L1 -> compile_l1_counter;
            case L2 -> compile_l2_counter;
        };
        return counter.getOrDefault(methodID, 0);
    }

    private CompiledMethod compilationProcess(CompilationLevel compilationLevel, MethodID methodID) {
        // This lock doesn't break Weak-worst-case-latency constraint because it doesn't happen
        // inside threads which perform executeMethod(id)
        try {
            // Condition with mutex for case when we got at least two parallel tasks to compile the method with the exact id
            // we allow 1 compilation depending on the level. Requests left are skipped (via returning null)
            lock.lock();
            var l_counter = get(compilationLevel, methodID);
            if (l_counter <= 0) {
                increment(compilationLevel, methodID);
            } else {
                return null;
            }

        } finally {
            lock.unlock();
        }

        return switch (compilationLevel) {
            case L1 -> compilationEngine.compile_l1(methodID);
            case L2 -> compilationEngine.compile_l2(methodID);
        };
    }

    public Future<CompiledMethod> compile(CompilationLevel compilationLevel, MethodID methodID) {
        return service.submit(() -> compilationProcess(compilationLevel, methodID));
    }
}

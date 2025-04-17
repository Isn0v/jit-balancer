package org.nsu.syspro.parprog;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SolutionThread;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CompilationThreadPool {
    private final CompilationEngine compilationEngine;

    private static HashMap<MethodID, Integer> compile_l1_counter = new HashMap<>();
    private static HashMap<MethodID, Integer> compile_l2_counter = new HashMap<>();
    private static ReadWriteLock lock = new ReentrantReadWriteLock();

    private final ExecutorService service;

    public CompilationThreadPool(CompilationEngine compilationEngine, int compilationThreadBound) {
        this.compilationEngine = compilationEngine;
        service = Executors.newFixedThreadPool(compilationThreadBound);
    }

    private static void increment(SolutionThread.CompilationLevel compilationLevel, MethodID methodID){
        var counter = switch (compilationLevel){
            case L1 -> compile_l1_counter;
            case L2 -> compile_l2_counter;
        };
        try {
            lock.writeLock().lock();
            if (counter.containsKey(methodID)){
                var current = counter.get(methodID) + 1;
                counter.put(methodID, current);
            } else {
                counter.put(methodID, 1);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static void decrement(SolutionThread.CompilationLevel compilationLevel, MethodID methodID){
        var counter = switch (compilationLevel){
            case L1 -> compile_l1_counter;
            case L2 -> compile_l2_counter;
        };
        try {
            lock.writeLock().lock();
            if (counter.containsKey(methodID)){
                var current = counter.get(methodID) - 1;
                assert current >= 0 : "Counter value of compiling method must be greater ot equal to 0";
                counter.put(methodID, current);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static Optional<Integer> get(SolutionThread.CompilationLevel compilationLevel, MethodID methodID){
        var counter = switch (compilationLevel){
            case L1 -> compile_l1_counter;
            case L2 -> compile_l2_counter;
        };
        try {
            lock.readLock().lock();
            return Optional.ofNullable(counter.getOrDefault(methodID, null));
        } finally {
            lock.readLock().unlock();
        }
    }

    public synchronized CompiledMethod compile(SolutionThread.CompilationLevel compilationLevel, MethodID methodID) {
        Callable<CompiledMethod> compileCallable = () -> switch (compilationLevel) {
            case L1: {
                while (true){
                    if (get(SolutionThread.CompilationLevel.L1, methodID).isEmpty() ||
                            get(SolutionThread.CompilationLevel.L1, methodID).get() <= 1) {
                        break;
                    } else {
                        Thread.sleep(100);
                    }
                }
                increment(SolutionThread.CompilationLevel.L1, methodID);
                var code =  compilationEngine.compile_l1(methodID);
                decrement(SolutionThread.CompilationLevel.L1, methodID);
                yield code;
            }
            case L2: {
                while (true){
                    if (get(SolutionThread.CompilationLevel.L2, methodID).isEmpty() ||
                            get(SolutionThread.CompilationLevel.L2, methodID).get() <= 0) {
                        break;
                    } else {
                        Thread.sleep(100);
                    }
                }
                increment(SolutionThread.CompilationLevel.L2, methodID);
                var code =  compilationEngine.compile_l2(methodID);
                decrement(SolutionThread.CompilationLevel.L2, methodID);
                yield code;
            }
        };

        var futureResult = service.submit(compileCallable);
        CompiledMethod code;
        try {
            code = futureResult.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return code;
    }
}

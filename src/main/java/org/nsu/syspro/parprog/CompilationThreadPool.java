package org.nsu.syspro.parprog;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SolutionThread;

import java.util.concurrent.*;

public class CompilationThreadPool {
    private final CompilationEngine compilationEngine;


    private final ExecutorService service;

    public CompilationThreadPool(CompilationEngine compilationEngine, int compilationThreadBound) {
        this.compilationEngine = compilationEngine;
        service = Executors.newFixedThreadPool(compilationThreadBound);
    }


    public synchronized CompiledMethod compile(SolutionThread.CompilationLevel compilationLevel, MethodID methodID) {
        Callable<CompiledMethod> compileCallable = () -> switch (compilationLevel) {
            case L1 -> compilationEngine.compile_l1(methodID);
            case L2 -> compilationEngine.compile_l2(methodID);
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

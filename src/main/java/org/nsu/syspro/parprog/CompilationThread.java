package org.nsu.syspro.parprog;

import org.nsu.syspro.parprog.external.CompilationEngine;
import org.nsu.syspro.parprog.external.CompiledMethod;
import org.nsu.syspro.parprog.external.MethodID;
import org.nsu.syspro.parprog.solution.SolutionThread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CompilationThread {
    private final CompilationEngine compilationEngine;
    private final SolutionThread.CompilationLevel level;
    private final MethodID methodID;


    private final ExecutorService service = Executors.newSingleThreadExecutor();

    public CompilationThread(CompilationEngine compilationEngine, SolutionThread.CompilationLevel level, MethodID methodID) {
        this.compilationEngine = compilationEngine;
        this.level = level;
        this.methodID = methodID;
    }


    public CompiledMethod compile() {
        Callable<CompiledMethod> compileCallable = () -> switch (level) {
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

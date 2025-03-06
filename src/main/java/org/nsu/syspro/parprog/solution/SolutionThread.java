package org.nsu.syspro.parprog.solution;

import org.nsu.syspro.parprog.UserThread;
import org.nsu.syspro.parprog.external.*;

import java.util.*;
import java.util.concurrent.*;

public class SolutionThread extends UserThread {

    // TODO: add fields here!

    public SolutionThread(int compilationThreadBound, ExecutionEngine exec, CompilationEngine compiler, Runnable r) {
        super(compilationThreadBound, exec, compiler, r);
        // TODO: initialize fields!
    }


    @Override
    public ExecutionResult executeMethod(MethodID id) {
        // TODO: implement me
        return null;
    }

    // TODO: add methods
    // TODO: add inner classes
    // TODO: add utility classes in the same package
}
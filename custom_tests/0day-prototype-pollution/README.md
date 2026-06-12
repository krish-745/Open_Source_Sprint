# Prototype Pollution Hunt (Joern)

This directory contains the custom Joern script to mathematically prove a taint flow from external inputs (API routes) into the `Object.assign` sink in `task-queue.ts`.

## How to run this

1. **Generate the Code Property Graph (CPG):**
   Run the JavaScript/TypeScript frontend to parse the source code.
   ```bash
   jssrc2cpg ../../src/ -o app.bin
   ```

2. **Execute the Joern Query:**
   Load the CPG and run the custom Scala script to perform the data-flow analysis.
   ```bash
   joern --script taint_query.sc --params cpgFile=app.bin
   ```

If the query outputs a flow path, it proves that an attacker can pass `__proto__` via an API request directly into `Object.assign`, leading to a confirmed Remote Code Execution (RCE) / Denial of Service (DoS) 0-day!

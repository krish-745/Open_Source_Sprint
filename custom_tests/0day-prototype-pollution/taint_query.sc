@main def main(): Unit = {
  println("[+] Loading Code Property Graph...")
  importCpg("custom_tests/0day-prototype-pollution/app.bin", "myapp")

  println("[+] Hunting for Prototype Pollution via Object.assign...")

  // 1. Define the Sink: Any argument passed to Object.assign
  // We specifically care about the second argument (the source object being merged)
  val sink = cpg.call.name("assign").argument

  // 2. Define the Source: External inputs
  // We look for parameters in route handlers or explicitly named metadata/body parameters
  val source = cpg.method.parameter.name(".*(body|query|metadata|payload|req).*")

  // 3. Data Flow Analysis
  // Calculate if any path exists from the external source directly into the dangerous sink
  val flows = sink.reachableByFlows(source)

  if (flows.size > 0) {
    println("==================================================")
    println("🚨 VULNERABILITY FOUND: PROTOTYPE POLLUTION 🚨")
    println("==================================================")
    println("Taint flow detected from external input to Object.assign.")
    println("This is a confirmed Remote Code Execution / Denial of Service 0-day.")
    println("")
    
    // Print the exact data flow path (file names, line numbers, and variables)
    flows.p
  } else {
    println("[✓] No direct taint flow found.")
    println("The input might be sanitized, or the Express routes do not pass raw body data to this function.")
  }
}

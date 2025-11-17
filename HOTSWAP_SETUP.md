# Kotlin Hot Reload with JBR + HotswapAgent

This document describes the Kotlin hot reload setup for XTDB2 development, enabling iterative development across Kotlin and Clojure without constant REPL restarts.

## Benefits

### Before: Traditional Workflow
- **Kotlin change** → Full REPL restart (~30 seconds)
- **Clojure change** → `(reset)` (~5 seconds)
- Frequent Kotlin changes = lots of waiting

### After: Hot Reload Workflow
- **Kotlin change** → Automatic hot reload (~7 seconds)
- **Clojure change** → `(reset)` (~5 seconds) - unchanged
- **No REPL restart needed** for most Kotlin development

### Key Improvements
- ✅ **Save ~23 seconds** per Kotlin iteration (30s → 7s)
- ✅ **Keep REPL state** - no need to rebuild test data
- ✅ **Faster iteration** - especially valuable for Claude Code workflows making many experimental changes
- ✅ **Works for most changes** - method bodies, signatures, new methods, new fields, even new classes

## What Hot Reloads Without REPL Restart

### Fully Supported ✅
1. **Method body changes** - Modify implementation logic, return values, control flow
2. **Adding new methods** - New methods are immediately callable
3. **Changing method signatures** - Add/remove parameters, change types
4. **Adding new fields** - New fields immediately accessible
5. **Adding new classes** - Entirely new `.kt` files work immediately

### Still Requires REPL Restart ❌
- ANTLR grammar changes (generates Java code)
- Protobuf changes (generates Java code)
- Dependency changes (requires classpath modification)
- Module configuration changes

## Installation

### 1. Download HotswapAgent

```bash
cd /home/jdt/ghq/github.com/xtdb/xtdb2
mkdir -p dev/hotswap
cd dev/hotswap

# Download latest HotswapAgent
wget https://github.com/HotswapProjects/HotswapAgent/releases/download/1.4.2-SNAPSHOT/hotswap-agent-1.4.2-SNAPSHOT.jar
```

### 2. Install HotswapAgent into JBR

```bash
# Copy to JBR's lib/hotswap directory (must be named hotswap-agent.jar)
mkdir -p /path/to/jbr/lib/hotswap
cp dev/hotswap/hotswap-agent-1.4.2-SNAPSHOT.jar /path/to/jbr/lib/hotswap/hotswap-agent.jar
```

For XTDB2 with local JBR:
```bash
mkdir -p jbr_jcef-21.0.9-linux-x64-b895.147/lib/hotswap
cp dev/hotswap/hotswap-agent-1.4.2-SNAPSHOT.jar jbr_jcef-21.0.9-linux-x64-b895.147/lib/hotswap/hotswap-agent.jar
```

### 3. Set JAVA_HOME to JBR

**IMPORTANT**: You must set `JAVA_HOME` to point to JBR for Gradle to use it.

For XTDB2 with local JBR (add to your shell profile):
```bash
export JAVA_HOME=/home/jdt/ghq/github.com/xtdb/xtdb2/jbr_jcef-21.0.9-linux-x64-b895.147
```

Or set it per-session:
```bash
# In the terminal where you'll run Gradle commands
export JAVA_HOME=/path/to/xtdb2/jbr_jcef-21.0.9-linux-x64-b895.147

# Verify it's set correctly
java -version  # Should show "JetBrains Runtime"
```

### 4. Configuration Already in build.gradle.kts

The configuration is already set up in `build.gradle.kts` lines 235-253:

```kotlin
if (project.hasProperty("hotswap")) {
    // Use JBR's built-in HotswapAgent in fatjar mode
    jvmArgs += "-XX:+AllowEnhancedClassRedefinition"
    jvmArgs += "-XX:HotswapAgent=fatjar"
    jvmArgs += "-Xlog:redefine+class*=info"  // Optional: see reload logs

    // Configure directories to watch
    val coreBuildDir = rootProject.file("core/build/classes/kotlin/main").absolutePath
    val apiBuildDir = rootProject.file("api/build/classes/kotlin/main").absolutePath
    jvmArgs += "-DextraClasspath=${coreBuildDir};${apiBuildDir}"
    jvmArgs += "-DautoHotswap=true"
}
```

## Usage

### Three Terminal Workflow

**Terminal 1: Continuous Kotlin Compilation**
```bash
./gradlew -t :xtdb-core:compileKotlin :xtdb-api:compileKotlin
```

**Terminal 2: REPL with Hot Reload**
```bash
./gradlew :clojureRepl -PreplPort=7882 -Photswap
```

**Terminal 3: Claude Code (start after REPL is running)**
```bash
claude
```

### Development Flow

1. Make Kotlin changes in your editor or via Claude Code
2. Wait ~7 seconds for hot reload (Gradle will auto-compile, HotswapAgent will auto-reload)
3. Test immediately in REPL - no restart needed!
4. Make Clojure changes and use `(reset)` as normal

### Verification

Check HotswapAgent loaded correctly:
```clojure
;; In REPL
(try
  (Class/forName "org.hotswap.agent.HotswapAgent")
  "HotswapAgent loaded!"
  (catch ClassNotFoundException e
    "HotswapAgent not found"))
;; => "HotswapAgent loaded!"
```

## Performance Characteristics

### Hot Reload Timing
- **Roundtrip time**: ~7-8 seconds from file save to usable in REPL
  - Gradle compilation: ~4-5 seconds (file change detection + compilation)
  - HotswapAgent reload: ~2-3 seconds (class reload)
- **REPL startup**: Slightly slower (+2-3 seconds overhead)
- **Runtime overhead**: Negligible

### Comparison
- **Traditional REPL restart**: ~30 seconds
- **With hot reload**: ~7 seconds
- **Speedup**: ~4x faster iteration

## Troubleshooting

### HotswapAgent not loading

**Symptom**: No "HotswapAgent loaded!" message

**Check**:
1. Verify jar is in correct location: `ls <JBR>/lib/hotswap/hotswap-agent.jar`
2. Verify filename is exactly `hotswap-agent.jar` (not including version)
3. Check REPL started with `-Photswap` flag
4. Restart REPL after installing jar

### Changes not hot reloading

**Symptom**: Kotlin changes don't take effect

**Check**:
1. Is continuous compilation running? `ps aux | grep "gradle.*-t.*compileKotlin"`
2. Did compilation succeed? Check Terminal 1 for errors
3. Wait full 8 seconds after file save
4. Check class file timestamp: `stat core/build/classes/kotlin/main/xtdb/util/YourClass.class`

### Still need to restart for some changes

This is expected! Changes requiring restart:
- ANTLR grammar (`.g4` files)
- Protobuf (`.proto` files)
- Dependencies (`build.gradle.kts` dependencies section)
- Java code changes

## Technical Details

### How It Works

1. **JetBrains Runtime (JBR)**: Fork of OpenJDK with enhanced class redefinition support
2. **HotswapAgent**: Agent that watches for `.class` file changes and triggers JVM redefinition
3. **Gradle Continuous Build**: Automatically recompiles Kotlin files when they change
4. **Enhanced Class Redefinition**: JVM flag `-XX:+AllowEnhancedClassRedefinition` enables DCEVM (Dynamic Code Evolution VM) support

### Limitations vs Standard JVM

Standard JVM hotswap (JVMTI) only allows:
- Method body changes (no signature changes)

JBR + HotswapAgent allows:
- Method body changes ✅
- Method signature changes ✅
- Adding methods ✅
- Adding fields ✅
- Adding classes ✅
- Changing class hierarchy ⚠️ (not tested, may work)

## Resources

- [HotswapAgent Documentation](https://hotswapagent.org/)
- [JetBrains Runtime GitHub](https://github.com/JetBrains/JetBrainsRuntime)
- [HotswapAgent Quick Start - JDK 17+](https://hotswapagent.org/mydoc_quickstart-jdk17.html)

## Notes for Claude Code Workflows

This setup is particularly valuable when using Claude Code for iterative development:

1. **Experimental changes**: Try multiple approaches to Kotlin code without restart penalty
2. **Cross-language refactoring**: Modify both Kotlin and Clojure in same session
3. **Debugging**: Tweak implementations and immediately test without losing REPL state
4. **Test-driven development**: Modify Kotlin implementation → test in REPL → refine → repeat

The ~7 second hot reload time is acceptable for human-paced development and much faster than the alternative 30+ second restart cycle.

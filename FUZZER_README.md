# Aerospike Command Fuzzer

This fuzzer allows you to inject random mutations into Aerospike commands before they are sent to the server. This is useful for testing the robustness of both the client and server under various error conditions.

## Files

- `src/include/aerospike/fuzzer.h` - Header file with function declarations
- `src/main/aerospike/fuzzer.c` - Implementation of the fuzzing functionality
- `src/main/aerospike/as_command.c` - Modified to include and call the fuzzer

## How It Works

The fuzzer intercepts commands in `as_command_send()` after the command buffer is populated but before it's sent to the server. It can then apply various mutation strategies to corrupt the data.

## Fuzzing Strategies

The fuzzer randomly applies one of these mutation strategies to each byte:

1. **Bit flip** - Flips a random bit in the byte
2. **Random byte** - Replaces the byte with a completely random value
3. **Add/subtract** - Adds or subtracts a small value (-10 to +10)
4. **Zero out** - Sets the byte to zero

## Configuration

### Environment Variables (Recommended)

Set these environment variables before starting your application:

```bash
export AEROSPIKE_FUZZ_ENABLE=1                    # Enable fuzzing
export AEROSPIKE_FUZZ_PROBABILITY=0.01            # 1% chance per byte (0.0 to 1.0)
```

### Programmatic Control

If you have access to the C functions, you can control the fuzzer directly:

```c
#include <aerospike/fuzzer.h>

// Enable fuzzing
fuzz_set_enabled(true);

// Set probability (0.0 = never, 1.0 = always fuzz each byte)
fuzz_set_probability(0.05);  // 5% chance per byte
```

## Usage Examples

### From Python Tests

```python
import os

def test_with_fuzzing():
    # Enable fuzzing
    os.environ["AEROSPIKE_FUZZ_ENABLE"] = "1"
    os.environ["AEROSPIKE_FUZZ_PROBABILITY"] = "0.02"  # 2% chance
    
    try:
        # Your test operations here
        client.put(key, data)
        result = client.get(key)
    finally:
        # Clean up
        os.environ.pop("AEROSPIKE_FUZZ_ENABLE", None)
        os.environ.pop("AEROSPIKE_FUZZ_PROBABILITY", None)
```

### From Command Line

```bash
# Run with fuzzing enabled
AEROSPIKE_FUZZ_ENABLE=1 AEROSPIKE_FUZZ_PROBABILITY=0.01 python test_script.py

# Or with pytest
AEROSPIKE_FUZZ_ENABLE=1 AEROSPIKE_FUZZ_PROBABILITY=0.05 pytest test_file.py -v -s
```

## Output

When fuzzing is active, you'll see output like:

```
Fuzzer: ENABLED via environment variable
Fuzzer: probability set to 0.050 via environment variable
Sending command (uncompressed-size: 64):
	as16010000000000000000160000000000000000000000030003000e74657374000000000000000074657374000100000001610001
Fuzzer: mutated 3 bytes in 64 byte buffer
```

## Safety Considerations

- **Start with low probabilities** (0.001 to 0.01) to avoid overwhelming the system
- **Use in test environments only** - never in production
- **Monitor server logs** for any crash or error indicators
- **Be prepared for connection failures** as corrupted commands may close connections

## Debugging

The fuzzer outputs debug information to stderr, including:
- When it's enabled/disabled
- Probability settings
- Number of bytes mutated per command

The original command hex dump is also displayed before fuzzing for comparison.

## Integration Notes

The fuzzer is automatically called from `as_command_send()` for all commands. It only activates when explicitly enabled, so it's safe to leave the code in place. 
import asyncio
import io
import sys
import time
import traceback
from typing import Any, Optional


SAFE_BUILTINS = {
    "print": print,
    "len": len,
    "range": range,
    "enumerate": enumerate,
    "zip": zip,
    "map": map,
    "filter": filter,
    "sorted": sorted,
    "reversed": reversed,
    "list": list,
    "dict": dict,
    "set": set,
    "tuple": tuple,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "type": type,
    "isinstance": isinstance,
    "hasattr": hasattr,
    "getattr": getattr,
    "min": min,
    "max": max,
    "sum": sum,
    "abs": abs,
    "round": round,
    "None": None,
    "True": True,
    "False": False,
    "Exception": Exception,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "KeyError": KeyError,
    "IndexError": IndexError,
    "AttributeError": AttributeError,
}


def _run_in_sandbox(code: str, entry: str, params: dict) -> dict:
    """Synchronous sandbox execution. Called in thread pool."""
    logs = []
    output = None
    error = None
    error_line = None

    # Capture print output
    captured_output = io.StringIO()
    original_stdout = sys.stdout

    def safe_print(*args, **kwargs):
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        line = sep.join(str(a) for a in args) + end
        logs.append(line.rstrip("\n"))
        captured_output.write(line)

    safe_builtins = dict(SAFE_BUILTINS)
    safe_builtins["print"] = safe_print

    namespace = {
        "__builtins__": safe_builtins,
        "__name__": "__sandbox__",
    }

    start_ms = int(time.time() * 1000)

    try:
        # Compile and execute the code
        compiled = compile(code, "<sandbox>", "exec")
        exec(compiled, namespace)  # noqa: S102

        # Check that entry function exists
        if entry not in namespace:
            error = f"Entry function '{entry}' not found in code"
            return {
                "valid": False,
                "output": None,
                "error": error,
                "error_line": None,
                "logs": logs,
                "duration_ms": int(time.time() * 1000) - start_ms,
            }

        func = namespace[entry]
        if not callable(func):
            error = f"'{entry}' is not callable"
            return {
                "valid": False,
                "output": None,
                "error": error,
                "error_line": None,
                "logs": logs,
                "duration_ms": int(time.time() * 1000) - start_ms,
            }

        output = func(**params)

        return {
            "valid": True,
            "output": output,
            "error": None,
            "error_line": None,
            "logs": logs,
            "duration_ms": int(time.time() * 1000) - start_ms,
        }

    except SyntaxError as e:
        error = f"SyntaxError: {e.msg}"
        error_line = e.lineno
        return {
            "valid": False,
            "output": None,
            "error": error,
            "error_line": error_line,
            "logs": logs,
            "duration_ms": int(time.time() * 1000) - start_ms,
        }
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        # Find the last frame from the sandbox code
        error_line = None
        for frame in reversed(tb):
            if frame.filename == "<sandbox>":
                error_line = frame.lineno
                break
        error = f"{type(e).__name__}: {str(e)}"
        return {
            "valid": False,
            "output": None,
            "error": error,
            "error_line": error_line,
            "logs": logs,
            "duration_ms": int(time.time() * 1000) - start_ms,
        }
    finally:
        sys.stdout = original_stdout


async def execute_script(
    code: str,
    entry: str,
    params: dict,
    timeout_seconds: int = 10,
) -> dict:
    """
    Execute a script in a restricted sandbox asynchronously.

    Returns:
        {
            "valid": bool,
            "output": any,
            "error": str | None,
            "error_line": int | None,
            "logs": [],
            "duration_ms": int
        }
    """
    loop = asyncio.get_event_loop()

    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _run_in_sandbox, code, entry, params),
            timeout=timeout_seconds,
        )
        return result
    except asyncio.TimeoutError:
        return {
            "valid": False,
            "output": None,
            "error": f"Execution timed out after {timeout_seconds} seconds",
            "error_line": None,
            "logs": [],
            "duration_ms": timeout_seconds * 1000,
        }
